-- loan_fintech_schema.sql
-- PostgreSQL schema for Balance-sheet & Marketplace lending
-- Assumes PostgreSQL 12+ (uses gen_random_uuid() from pgcrypto)
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

---------------------------
-- ENUM / DOMAIN TYPES
---------------------------

CREATE TYPE user_role AS ENUM ('admin','ops','underwriter','support','investor');
CREATE TYPE application_status AS ENUM ('initiated','submitted','underwriting','approved','declined','withdrawn');
CREATE TYPE offer_status AS ENUM ('pending','accepted','rejected','expired');
CREATE TYPE loan_status AS ENUM ('funded','active','late','defaulted','paid_off','charged_off','written_off');
CREATE TYPE repayment_status AS ENUM ('due','paid','partial','failed','waived');
CREATE TYPE payment_status AS ENUM ('pending','settled','failed','reversed','cancelled');
CREATE TYPE disbursement_status AS ENUM ('pending','completed','failed','cancelled');
CREATE TYPE transaction_type AS ENUM ('disbursement','repayment','fee','refund','chargeback','investment_funding','investment_return');
CREATE TYPE kyc_status AS ENUM ('pending','passed','failed','manual_review');
CREATE TYPE webhook_status AS ENUM ('pending','sent','failed','dead');
CREATE TYPE event_type AS ENUM ('first_notice','late_fee_applied','payment_plan_offered','chargeoff','reinstatement');
CREATE TYPE fee_applies_on AS ENUM ('origination','late_payment','monthly','prepayment');
CREATE TYPE funding_model AS ENUM ('balance_sheet','marketplace');


---------------------------
-- CORE TABLES
---------------------------

CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT UNIQUE,
  email_ciphertext BYTEA,
  password_hash TEXT,
  role user_role NOT NULL DEFAULT 'support',
  name_first TEXT,
  name_last TEXT,
  status TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  last_login_at TIMESTAMPTZ
);

CREATE INDEX idx_users_role ON users(role);

CREATE TABLE customers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  external_id TEXT,
  email TEXT,
  email_ciphertext BYTEA,
  phone TEXT,
  phone_ciphertext BYTEA,
  name_first TEXT,
  name_last TEXT,
  dob DATE,
  ssn_hash TEXT,
  preferred_contact JSONB,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_external_id ON customers(external_id);

CREATE TABLE kyc_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID REFERENCES customers(id) ON DELETE CASCADE,
  kyc_provider TEXT,
  status kyc_status NOT NULL DEFAULT 'pending',
  result JSONB,
  scanned_at TIMESTAMPTZ,
  reviewed_at TIMESTAMPTZ,
  reviewer_id UUID REFERENCES users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_kyc_customer ON kyc_records(customer_id);

CREATE TABLE documents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID REFERENCES customers(id) ON DELETE CASCADE,
  application_id UUID,
  type TEXT,
  url TEXT,
  checksum TEXT,
  storage_provider TEXT,
  uploaded_by UUID REFERENCES users(id) ON DELETE SET NULL,
  uploaded_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_documents_customer ON documents(customer_id);

CREATE TABLE bank_accounts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID REFERENCES customers(id) ON DELETE CASCADE,
  bank_name TEXT,
  account_token TEXT,
  last4 TEXT,
  account_type TEXT,
  routing_hash TEXT,
  verified BOOLEAN DEFAULT FALSE,
  verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_bankaccounts_customer ON bank_accounts(customer_id);

CREATE TABLE credit_scores (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID REFERENCES customers(id) ON DELETE CASCADE,
  provider TEXT,
  score INTEGER,
  report JSONB,
  fetched_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_credit_scores_customer ON credit_scores(customer_id);

---------------------------
-- LENDING FLOW: APPLICATIONS, OFFERS, LOANS
---------------------------

CREATE TABLE loan_applications (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID REFERENCES customers(id) ON DELETE CASCADE,
  application_reference TEXT UNIQUE,
  requested_amount_cents BIGINT NOT NULL,
  purpose TEXT,
  funding_model funding_model NOT NULL DEFAULT 'balance_sheet',
  status application_status NOT NULL DEFAULT 'initiated',
  decision_reason TEXT,
  submitted_at TIMESTAMPTZ,
  decision_at TIMESTAMPTZ,
  idempotency_key TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_applications_customer ON loan_applications(customer_id);
CREATE INDEX idx_applications_status ON loan_applications(status);

CREATE TABLE loan_offers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  application_id UUID REFERENCES loan_applications(id) ON DELETE CASCADE,
  offer_reference TEXT UNIQUE,
  principal_cents BIGINT,
  term_months INTEGER,
  interest_rate_pct NUMERIC(6,4),
  monthly_payment_cents BIGINT,
  fees_cents BIGINT DEFAULT 0,
  status offer_status DEFAULT 'pending',
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_offers_application ON loan_offers(application_id);
CREATE INDEX idx_offers_status ON loan_offers(status);

CREATE TABLE loans (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  loan_number TEXT UNIQUE,
  customer_id UUID REFERENCES customers(id) ON DELETE SET NULL,
  offer_id UUID REFERENCES loan_offers(id) ON DELETE SET NULL,
  principal_cents BIGINT NOT NULL,
  outstanding_balance_cents BIGINT NOT NULL,
  term_months INTEGER,
  interest_rate_pct NUMERIC(6,4),
  status loan_status NOT NULL DEFAULT 'funded',
  funding_model funding_model NOT NULL DEFAULT 'balance_sheet',
  disbursed_at TIMESTAMPTZ,
  maturity_date DATE,
  next_payment_due_date DATE,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_loans_customer ON loans(customer_id);
CREATE INDEX idx_loans_status ON loans(status);

CREATE TABLE repayment_schedules (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  loan_id UUID REFERENCES loans(id) ON DELETE CASCADE,
  due_date DATE NOT NULL,
  period_number INTEGER NOT NULL,
  amount_due_cents BIGINT NOT NULL,
  principal_cents BIGINT,
  interest_cents BIGINT,
  fees_cents BIGINT DEFAULT 0,
  status repayment_status DEFAULT 'due',
  paid_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_repayment_loan_due ON repayment_schedules(loan_id, due_date);
CREATE INDEX idx_repayment_status ON repayment_schedules(status);

---------------------------
-- PAYMENTS / DISBURSEMENTS / TRANSACTIONS
---------------------------

CREATE TABLE payments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  loan_id UUID REFERENCES loans(id) ON DELETE SET NULL,
  customer_id UUID REFERENCES customers(id) ON DELETE SET NULL,
  payment_method TEXT,
  amount_cents BIGINT NOT NULL,
  status payment_status DEFAULT 'pending',
  provider_txn_id TEXT,
  idempotency_key TEXT,
  attempt_count INTEGER DEFAULT 0,
  last_error TEXT,
  next_retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  settled_at TIMESTAMPTZ
);

CREATE INDEX idx_payments_loan ON payments(loan_id);
CREATE INDEX idx_payments_customer ON payments(customer_id);
CREATE INDEX idx_payments_status ON payments(status);
CREATE UNIQUE INDEX uq_payments_idempotency ON payments(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE transactions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  loan_id UUID REFERENCES loans(id) ON DELETE SET NULL,
  origin_account TEXT NOT NULL,
  destination_account TEXT NOT NULL,
  amount_cents BIGINT NOT NULL,
  type transaction_type NOT NULL,
  status TEXT,
  reference TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_transactions_loan ON transactions(loan_id);
CREATE INDEX idx_transactions_type ON transactions(type);

CREATE TABLE disbursements (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  loan_id UUID REFERENCES loans(id) ON DELETE SET NULL,
  amount_cents BIGINT NOT NULL,
  method TEXT,
  provider_reference TEXT,
  status disbursement_status DEFAULT 'pending',
  attempts INTEGER DEFAULT 0,
  last_error TEXT,
  next_retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  completed_at TIMESTAMPTZ
);

CREATE INDEX idx_disbursements_loan ON disbursements(loan_id);
CREATE INDEX idx_disbursements_status ON disbursements(status);

---------------------------
-- FEES, COLLECTIONS, VALIDATION, WEBHOOKS, AUDIT
---------------------------

CREATE TABLE fee_schedule (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  description TEXT,
  amount_cents BIGINT,
  rate_pct NUMERIC(6,4),
  applies_on fee_applies_on,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_fee_schedule_active ON fee_schedule(active);

CREATE TABLE collections_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  loan_id UUID REFERENCES loans(id) ON DELETE CASCADE,
  event_type event_type NOT NULL,
  status TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  resolved_at TIMESTAMPTZ
);

CREATE INDEX idx_collections_loan ON collections_events(loan_id);
CREATE INDEX idx_collections_type ON collections_events(event_type);

CREATE TABLE validation_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_table TEXT NOT NULL,
  source_row_id UUID NOT NULL,
  rule_id TEXT NOT NULL,
  result BOOLEAN NOT NULL,
  details JSONB,
  checked_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_validation_source ON validation_results(source_table, source_row_id);

CREATE TABLE webhooks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  target_url TEXT NOT NULL,
  payload JSONB,
  event_type TEXT,
  status webhook_status DEFAULT 'pending',
  attempts INTEGER DEFAULT 0,
  last_error TEXT,
  next_retry_at TIMESTAMPTZ,
  idempotency_key TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_webhooks_status ON webhooks(status);

CREATE TABLE audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT,
  entity_id UUID,
  action TEXT,
  actor_id UUID REFERENCES users(id) ON DELETE SET NULL,
  changes JSONB,
  request_id TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_audit_entity ON audit_log(entity_type, entity_id);

CREATE TABLE idempotency_keys (
  key TEXT PRIMARY KEY,
  owner TEXT,
  response JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  expires_at TIMESTAMPTZ
);

CREATE INDEX idx_idempotency_owner ON idempotency_keys(owner);

---------------------------
-- INVESTOR / MARKETPLACE (marketplace lending tables)
---------------------------

CREATE TABLE investor_accounts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  account_reference TEXT UNIQUE,
  balance_cents BIGINT DEFAULT 0,
  reserved_cents BIGINT DEFAULT 0,
  status TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_investor_user ON investor_accounts(user_id);

CREATE TABLE investments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  investor_account_id UUID REFERENCES investor_accounts(id) ON DELETE CASCADE,
  loan_id UUID REFERENCES loans(id) ON DELETE RESTRICT,
  amount_cents BIGINT NOT NULL,
  allocation_percentage NUMERIC(8,6),
  created_at TIMESTAMPTZ DEFAULT now(),
  distributed BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_investments_loan ON investments(loan_id);
CREATE INDEX idx_investments_investor ON investments(investor_account_id);

CREATE TABLE investor_funding_movements (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  investor_account_id UUID REFERENCES investor_accounts(id) ON DELETE CASCADE,
  amount_cents BIGINT NOT NULL,
  type transaction_type NOT NULL,
  reference TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_funding_movements_investor ON investor_funding_movements(investor_account_id);

CREATE TABLE marketplace_reserves (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  total_committed_cents BIGINT DEFAULT 0,
  total_available_cents BIGINT DEFAULT 0,
  updated_at TIMESTAMPTZ DEFAULT now()
);

---------------------------
-- BALANCE-SHEET SPECIFIC (platform funding & capital accounts)
---------------------------

CREATE TABLE platform_accounts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT UNIQUE,
  account_type TEXT,
  balance_cents BIGINT DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_platform_accounts_type ON platform_accounts(account_type);

CREATE TABLE platform_funding_movements (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  platform_account_id UUID REFERENCES platform_accounts(id) ON DELETE SET NULL,
  loan_id UUID REFERENCES loans(id) ON DELETE SET NULL,
  amount_cents BIGINT NOT NULL,
  type transaction_type NOT NULL,
  reference TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_platform_funding_loan ON platform_funding_movements(loan_id);

---------------------------
-- MISC: indexing, partial indexes, and constraints
---------------------------

CREATE INDEX IF NOT EXISTS idx_loans_active ON loans (id) WHERE status = 'active';

ALTER TABLE loans ADD CONSTRAINT  chk_principal_nonnegative CHECK (principal_cents >= 0);
ALTER TABLE payments ADD CONSTRAINT chk_payment_amount_positive CHECK (amount_cents > 0);
ALTER TABLE repayment_schedules ADD CONSTRAINT chk_amount_due_positive CHECK (amount_due_cents >= 0);

---------------------------
-- FOREIGN KEY UPDATES (documents.application_id)
---------------------------

ALTER TABLE documents
  ADD COLUMN IF NOT EXISTS application_id UUID;

ALTER TABLE documents
  ADD CONSTRAINT IF NOT EXISTS fk_documents_application FOREIGN KEY (application_id)
  REFERENCES loan_applications(id) ON DELETE CASCADE;

