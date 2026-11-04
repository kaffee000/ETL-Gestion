# Archivo: dags/cne_cliente.py
"""
Cliente de la API CNE. No depende de Airflow, así que se puede probar por separado.

El token cambia constantemente, por lo que nunca se guarda: se obtiene con el login
(CNE_EMAIL y CNE_PASSWORD del entorno) y se renueva una vez si la API lo rechaza.
Tampoco pasa por XCom, para que no quede almacenado en la base de datos de Airflow.

La API responde HTTP 200 con un cuerpo como {"status": "Authorization Token not found"}
cuando hay un problema, así que no basta con revisar el código HTTP.
"""
import os
import time

import requests

from endpoints_cne import API_BASE, URL_LOGIN

CLAVES_MENSAJE = ("status", "message", "mensaje", "error")


class CNEAuthError(Exception):
    """Credenciales faltantes o rechazadas, o token que no se pudo renovar."""


class CNERespuestaError(Exception):
    """La API respondió con un mensaje en lugar de datos."""


def mensaje_error(payload):
    """Devuelve el texto si el JSON es un mensaje de la API y no datos; si no, None."""
    if not isinstance(payload, dict) or not payload:
        return None
    if not all(isinstance(v, str) for v in payload.values()):
        return None
    for clave in CLAVES_MENSAJE:
        if clave in payload:
            return payload[clave]
    return None


class ClienteCNE:
    def __init__(self, email, password, session=None, timeout=(10, 180)):
        if not email or not password:
            raise CNEAuthError("Faltan CNE_EMAIL o CNE_PASSWORD en el archivo .env.")
        self._email = email
        self._password = password
        self._session = session or requests.Session()
        self._timeout = timeout
        self._token = None

    @classmethod
    def desde_entorno(cls):
        return cls(os.getenv("CNE_EMAIL"), os.getenv("CNE_PASSWORD"))

    def login(self):
        """Pide un token nuevo. Credenciales rechazadas -> CNEAuthError (no conviene reintentar)."""
        r = self._session.post(URL_LOGIN, json={"email": self._email, "password": self._password},
                               headers={"Accept": "application/json"}, timeout=self._timeout)
        if r.status_code in (400, 401, 403, 422):
            raise CNEAuthError(f"La CNE rechazó el login (HTTP {r.status_code}).")
        r.raise_for_status()
        try:
            payload = r.json()
        except ValueError as e:
            raise CNERespuestaError("La respuesta del login no es JSON.") from e
        token = payload.get("token") if isinstance(payload, dict) else None
        if not token:
            detalle = mensaje_error(payload) or "la respuesta no trae token"
            raise CNEAuthError(f"La CNE rechazó el login: {detalle}.")
        self._token = token

    def _get(self, url):
        """GET con Bearer. Si el token fue rechazado, hace login una vez y repite."""
        if not self._token:
            self.login()
        for intento in (1, 2):
            r = self._session.get(url, headers={"Authorization": f"Bearer {self._token}",
                                                "Accept": "application/json"},
                                  timeout=self._timeout)
            if r.status_code == 429:  # límite de consultas: espera lo que indique la API
                espera = min(int(r.headers.get("Retry-After", 30)), 120)
                print(f"Aviso: la API pidió esperar {espera}s (HTTP 429).")
                time.sleep(espera)
                r = self._session.get(url, headers={"Authorization": f"Bearer {self._token}",
                                                    "Accept": "application/json"}, timeout=self._timeout)
            rechazado = r.status_code in (401, 403)
            detalle = f"HTTP {r.status_code}"
            payload = None
            if not rechazado:
                r.raise_for_status()
                try:
                    payload = r.json()
                except ValueError as e:
                    raise CNERespuestaError(f"{url} no devolvió JSON.") from e
                detalle = mensaje_error(payload)
                rechazado = bool(detalle) and "token" in detalle.lower()
            if not rechazado:
                if detalle:
                    raise CNERespuestaError(f"{url}: {detalle}")
                return payload
            if intento == 1:
                print(f"Aviso: token rechazado ({detalle}); se renueva con un login nuevo.")
                self.login()
        raise CNEAuthError(f"Token rechazado después de renovarlo: {detalle}.")

    def obtener(self, url):
        """Devuelve todos los datos del endpoint, siguiendo la paginación si existe."""
        payload = self._get(url)
        if not (isinstance(payload, dict) and isinstance(payload.get("data"), list)):
            return payload
        filas = list(payload["data"])
        pagina = payload
        while True:
            enlaces = pagina.get("links")
            siguiente = pagina.get("next_page_url") or (
                enlaces.get("next") if isinstance(enlaces, dict) else None)
            # El token solo se envía a la propia API
            if not siguiente or not str(siguiente).startswith(API_BASE):
                return filas
            pagina = self._get(siguiente)
            filas.extend(pagina.get("data") or [])
