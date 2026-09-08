from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status

from apps.ranking.config import Settings
from apps.ranking.models import ActiveDeal, ActiveDealPage
from apps.ranking.repository import DealRepository, PostgresDealRepository


def _repository(request: Request) -> DealRepository:
    return request.app.state.deal_repository


def create_app(repository: DealRepository | None = None) -> FastAPI:
    app = FastAPI(title="Grocery Savings Ranking API", version="0.1.0")
    app.state.deal_repository = repository or PostgresDealRepository(
        Settings.from_env().database_url
    )

    @app.get("/v1/deals", response_model=ActiveDealPage)
    async def list_active_deals(
        store_location_id: Annotated[str | None, Query(min_length=1, max_length=255)] = None,
        canonical_product_id: UUID | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
        deal_repository: DealRepository = Depends(_repository),
    ) -> ActiveDealPage:
        """Return only deals active at the server's current UTC time."""
        deals = await deal_repository.list_active(
            now=datetime.now(timezone.utc),
            store_location_id=store_location_id,
            canonical_product_id=canonical_product_id,
            limit=limit,
            offset=offset,
        )
        return ActiveDealPage(deals=list(deals), limit=limit, offset=offset)

    @app.get("/v1/deals/{deal_id}", response_model=ActiveDeal)
    async def get_active_deal(
        deal_id: UUID,
        deal_repository: DealRepository = Depends(_repository),
    ) -> ActiveDeal:
        """Return one active deal; expired and unknown deals are both hidden."""
        deal = await deal_repository.get_active(deal_id, now=datetime.now(timezone.utc))
        if deal is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deal not found")
        return deal

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready(
        response: Response,
        deal_repository: DealRepository = Depends(_repository),
    ) -> dict[str, str]:
        if not await deal_repository.ping():
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "unavailable"}
        return {"status": "ok"}

    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run("apps.ranking.api:app", host="0.0.0.0", port=8000)
