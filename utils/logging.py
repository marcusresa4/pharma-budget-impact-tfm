"""Configura logging en formato compacto UTC (una sola vez) y silencia ruido externo."""

from __future__ import annotations
import logging
import os
import sys
import time

_HANDLER_FLAG = "_is_pipeline_root_handler"


def setup_logging(level: str | None = None) -> logging.Logger:
    """Inicializa root logger (idempotente) y devuelve instancia."""
    root = logging.getLogger()

    # Determinar nivel (fallback seguro)
    level_name = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    lvl = getattr(logging, level_name, logging.INFO)
    root.setLevel(lvl)

    # ¿Ya hay nuestro handler?
    if not any(getattr(h, _HANDLER_FLAG, False) for h in root.handlers):
        fmt = "%(asctime)sZ | %(levelname)s | %(name)s | %(message)s"
        formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%dT%H:%M:%S")
        formatter.converter = time.gmtime  # UTC
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(formatter)
        setattr(h, _HANDLER_FLAG, True)
        root.addHandler(h)

        # Reducir ruido de librerías verbosas
        for noisy in ("urllib3", "requests"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    return root


def get_logger(name: str) -> logging.Logger:
    """Helper para obtener logger asegurando configuración previa."""
    setup_logging()
    return logging.getLogger(name)