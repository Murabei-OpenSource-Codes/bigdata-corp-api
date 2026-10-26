"""Auxiliary modules for BigDataCorp API."""

# Local imports
from bigdatacorp_api.aux.base import BaseBigDataCorpAPIMixin
from bigdatacorp_api.aux.person import PersonBigDataCorpAPIMixin
from bigdatacorp_api.aux.company import CompanyBigDataCorpAPIMixin
from bigdatacorp_api.aux.process import ProcessBigDataCorpAPIMixin
from bigdatacorp_api.aux.system import SystemBigDataCorpAPIMixin

__all__ = [
    "BaseBigDataCorpAPIMixin",
    "PersonBigDataCorpAPIMixin",
    "CompanyBigDataCorpAPIMixin",
    "ProcessBigDataCorpAPIMixin",
    "SystemBigDataCorpAPIMixin"]