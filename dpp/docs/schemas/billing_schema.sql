-- Organizations: 고객사의 구독 상태와 잔액 관리
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    api_key_hash VARCHAR(255) UNIQUE NOT NULL, -- 보안을 위해 해시 저장 권장
    
    -- Subscription Info
    subscription_tier VARCHAR(50) NOT NULL DEFAULT 'SANDBOX', -- 'SANDBOX', 'STARTER', 'GROWTH', 'ENTERPRISE'
    
    -- Billing Core
    credits_balance DECIMAL(19, 4) NOT NULL DEFAULT 500.0000, -- 소수점 4자리까지 정밀 관리
    soft_limit_threshold DECIMAL(19, 4) DEFAULT -50.0000, -- 이 금액만큼 마이너스가 되면 알림 (NULL이면 무제한)
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Usage Logs: 모든 과금 내역을 기록하는 불변 원장 (Immutable Ledger)
CREATE TABLE usage_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    run_id UUID NOT NULL, -- 실행 단위 ID (Traceability)
    
    -- Context
    autonomy_tier VARCHAR(10) NOT NULL, -- 'L1', 'L2', 'L3'
    base_cost DECIMAL(10, 4) NOT NULL DEFAULT 1.0000,
    multiplier DECIMAL(4, 2) NOT NULL, -- 1.0, 1.5, 3.0
    total_credits_deducted DECIMAL(19, 4) NOT NULL, -- base * multiplier
    
    -- Execution Result
    execution_status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILURE_BIZ' (4xx), 'FAILURE_SYS' (5xx)
    http_status_code INT, 
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast lookup & aggregation
CREATE INDEX idx_usage_logs_org_date ON usage_logs (organization_id, created_at);