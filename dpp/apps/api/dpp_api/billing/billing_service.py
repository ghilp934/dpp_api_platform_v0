from decimal import Decimal, ROUND_CEILING
from sqlalchemy.orm import Session
from sqlalchemy import select
from models import Organization, UsageLog  # 위 SQL에 대응하는 모델이라 가정
from errors import PaymentRequiredError, BusinessLogicError

# Tier별 Multiplier 정의 (정책 v0.2 반영)
TIER_MULTIPLIERS = {
    "L1": Decimal("1.0"),
    "L2": Decimal("1.5"),
    "L3": Decimal("3.0"),
}

class BillingService:
    def __init__(self, db: Session):
        self.db = db

    def process_transaction(self, org_id: str, run_id: str, requested_tier: str, execution_func):
        """
        핵심 과금 로직: Check -> Execute -> Deduct (Transaction)
        """
        # 1. 실행 전 권한/잔액 체크 (Pre-flight Check)
        # Lock을 걸어서 동시성 이슈 방지 (SELECT ... FOR UPDATE)
        org = self.db.execute(
            select(Organization).where(Organization.id == org_id).with_for_update()
        ).scalar_one()

        # Hard Limit Check (Sandbox only)
        if org.subscription_tier == 'SANDBOX' and org.credits_balance <= 0:
            raise PaymentRequiredError("Sandbox credits exhausted. Please upgrade.")

        # Soft Limit Alert (Starter/Growth) - 로깅만 하고 차단은 안 함
        if org.credits_balance < 0 and org.soft_limit_threshold:
             if abs(org.credits_balance) > abs(org.soft_limit_threshold):
                 print(f"[ALERT] Org {org.name} exceeded soft limit! Balance: {org.credits_balance}")

        # 2. 실제 비즈니스 로직 실행 (Execute)
        execution_status = 'SUCCESS'
        http_code = 200
        
        try:
            # 실제 에이전트 동작 수행
            result = execution_func() 
            return result
            
        except BusinessLogicError as e:
            # 4xx 에러: 비즈니스 거절 -> 과금 대상 (O)
            execution_status = 'FAILURE_BIZ'
            http_code = e.status_code
            raise e  # 에러를 다시 던져서 클라이언트에게 알림
            
        except Exception as e:
            # 5xx 에러: 시스템 장애 -> 과금 면제 (X)
            execution_status = 'FAILURE_SYS'
            http_code = 500
            raise e
            
        finally:
            # 3. 과금 처리 (Deduction) - finally 블록에서 수행하여 4xx 에러 시에도 실행 보장
            if execution_status != 'FAILURE_SYS':
                self._deduct_credits(org, run_id, requested_tier, execution_status, http_code)
                self.db.commit() # 최종 커밋

    def _deduct_credits(self, org, run_id, tier, status, code):
        base_cost = Decimal("1.0")
        multiplier = TIER_MULTIPLIERS.get(tier, Decimal("1.0"))
        
        # 소수점 처리: 개별 건은 정밀하게 계산 (월말 합산 시 올림 처리 추천)
        deduction_amount = base_cost * multiplier
        
        # 잔액 차감 (마이너스 허용)
        org.credits_balance -= deduction_amount
        
        # 로그 기록
        log = UsageLog(
            organization_id=org.id,
            run_id=run_id,
            autonomy_tier=tier,
            base_cost=base_cost,
            multiplier=multiplier,
            total_credits_deducted=deduction_amount,
            execution_status=status,
            http_status_code=code
        )
        self.db.add(log)