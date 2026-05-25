# Databricks notebook source
## DTL nao tem suporte para databricks free ou ce
## seria a primeira opcao mais pra atendimento aos requisitos do projeto

# COMMAND ----------

"""
Configurações base para Delta Live Tables
"""

from dataclasses import dataclass, field
from typing import Optional
import dlt


@dataclass
class DltTableConfig:
    name:    str
    comment: str = ""
    schema:  str = ""        # vazio = usa o target configurado no pipeline

    # table_properties comuns a todas as camadas
    optimize_write: bool = True
    auto_compact:   bool = True

    # sobrescritos pelas classes filhas
    quality:      str                  = "bronze"
    partition_by: Optional[list[str]]  = None
    expectations: Optional[dict]       = None

    def _base_props(self) -> dict:
        """propriedades delta compartilhadas por todas as camadas."""
        return {
            "delta.autoOptimize.optimizeWrite": str(self.optimize_write).lower(),
            "delta.autoOptimize.autoCompact":   str(self.auto_compact).lower(),
            "quality":                          self.quality,
        }

    def table_properties(self) -> dict:
        """
        extensível pelas classes filhas
        """
        return self._base_props()

    def to_dlt_decorator(self):
        """
        constrói e retorna o decorador @dlt.table configurado.
        """
        kwargs = dict(
            name             = self.name,
            comment          = self.comment,
            table_properties = self.table_properties(),
        )
        if self.schema:
            kwargs["schema"] = self.schema
        if self.partition_by:
            kwargs["partition_by"] = self.partition_by
        if self.expectations:
            kwargs["expect_all_or_drop"] = self.expectations

        return dlt.table(**kwargs)


@dataclass
class BronzeTableConfig(DltTableConfig):
    """
    ingestão bruta   via auto loader.
    dados originais sem  transformações.
    bronze não  particiona por padrão e não tem expectations.
    """

    quality: str = "bronze"

    def table_properties(self) -> dict:
        props = self._base_props()
        # Retenção estendida para auditoria e reprocessamento
        props["delta.logRetentionDuration"] = "interval 90 days"
        return props


@dataclass
class SilverTableConfig(DltTableConfig):
    """
    dados  limpos, tipados e com schema aplicado.
      habilita CDF para facilitar CDC nas camadas downstream.
    """

    quality:      str       = "silver"
    partition_by: list[str] = field(default_factory=list)
    expectations: dict      = field(default_factory=dict)

    def table_properties(self) -> dict:
        props = self._base_props()
        props["delta.enableChangeDataFeed"] = "true"
        return props


@dataclass
class GoldTableConfig(DltTableConfig):
    """
    agregados e KPIs prontos para consumo por BI e APIs.
    cdf habilitado para downstream incremental.
    """

    quality:  str       = "gold"
    partition_by: list[str] = field(default_factory=list)
    expectations: dict      = field(default_factory=dict)
    zorder_cols:   str   = ""    # colunas para Z-Order, separadas por vírgula

    def table_properties(self) -> dict:
        props = self._base_props()
        props["delta.enableChangeDataFeed"] = "true"
        if self.zorder_cols:
            props["pipelines.autoOptimize.zOrderCols"] = self.zorder_cols
        return props
