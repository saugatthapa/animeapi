import os

from .anizone_provider import AnizoneProvider


def _csv(value: str) -> list[str]:
    return [item.strip().lower() for item in (value or "").split(",") if item.strip()]


PRIMARY_STREAM_PROVIDER = os.getenv("PRIMARY_STREAM_PROVIDER", "anizone").strip().lower() or "anizone"
STREAM_PROVIDER_ORDER = _csv(
    os.getenv(
        "STREAM_PROVIDER_ORDER",
        "anizone,bonk,ally,dune,bee,hop,arc,zoro,jet,kiwi,animekai,mkissa",
    )
)


class ProviderResolver:
    def __init__(self):
        self.anizone = AnizoneProvider()
        self.providers = {self.anizone.name: self.anizone}

    def get(self, name: str):
        return self.providers.get((name or "").strip().lower())

    def order(self, provider_names):
        names = list(provider_names or [])
        preferred = []
        if PRIMARY_STREAM_PROVIDER and PRIMARY_STREAM_PROVIDER not in preferred:
            preferred.append(PRIMARY_STREAM_PROVIDER)
        for provider in STREAM_PROVIDER_ORDER:
            if provider not in preferred:
                preferred.append(provider)

        ordered = [name for name in preferred if name in names]
        ordered.extend(name for name in names if name not in ordered)
        return ordered
