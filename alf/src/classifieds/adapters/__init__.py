from src.classifieds.adapters.base import BaseClassifiedAdapter
from src.classifieds.adapters.rest import ClassifiedRestAdapter

CLASSIFIED_ADAPTER_REGISTRY: dict[str, type[BaseClassifiedAdapter]] = {
    ClassifiedRestAdapter.name: ClassifiedRestAdapter,
}

__all__ = ["BaseClassifiedAdapter", "ClassifiedRestAdapter", "CLASSIFIED_ADAPTER_REGISTRY"]
