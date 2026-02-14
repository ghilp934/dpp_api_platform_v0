from fastapi import FastAPI, Depends, Header, HTTPException
from sqlalchemy.orm import Session
from database import get_db
from billing_service import BillingService

app = FastAPI()

@app.post("/api/v1/agent/run")
def run_agent(
    payload: RunRequest,
    x_autonomy_tier: str = Header(..., alias="X-Autonomy-Tier"), # 헤더로 Tier 받음
    x_org_id: str = Header(..., alias="X-Org-ID"),
    db: Session = Depends(get_db)
):
    service = BillingService(db)
    
    # 실행할 실제 함수 (Closure)
    def agent_logic():
        # 여기에 실제 DPP 에이전트 로직 호출
        # 예: raise BusinessLogicError(400, "Insufficient funds") 테스트 가능
        return {"status": "completed", "result": "..."}

    try:
        # 과금 래퍼(Wrapper)를 통해 실행
        result = service.process_transaction(
            org_id=x_org_id,
            run_id=payload.request_id,
            requested_tier=x_autonomy_tier,
            execution_func=agent_logic
        )
        return result
        
    except HTTPException as e:
        raise e