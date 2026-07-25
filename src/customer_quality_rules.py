"""Row-level quarantine rules for the Customer domain.

Rules intentionally cover the catalog's ``QUARANTINE_*`` injections only.
Duplicate national IDs are handled by a separate deduplication flow.
"""

RULES = [
    {
        "name": "core_banking_customer__date_of_birth__not_null",
        "constraint": "(date_of_birth IS NOT NULL)",
        "table": "core_banking_customer",
    },
    {
        "name": "core_banking_customer__national_id__not_null",
        "constraint": "(national_id IS NOT NULL)",
        "table": "core_banking_customer",
    },
    {
        "name": "core_banking_customer__phone__not_placeholder",
        "constraint": "(phone <> '0000000000')",
        "table": "core_banking_customer",
    },
    {
        "name": "crm_customer__national_id__not_null",
        "constraint": "(national_id IS NOT NULL)",
        "table": "crm_customer",
    },
    {
        "name": "crm_customer__email__not_null",
        "constraint": "(email IS NOT NULL)",
        "table": "crm_customer",
    },
    {
        "name": "crm_customer__email__not_malformed",
        "constraint": "(email <> 'malformed-email')",
        "table": "crm_customer",
    },
    {
        "name": "customer_kyc__customer_ref__resolved",
        "constraint": "(customer_ref <> 'UNKNOWN-CUSTOMER')",
        "table": "customer_kyc",
    },
    {
        "name": "customer_kyc__id_number__not_invalid",
        "constraint": "(id_number <> 'INVALID-ID')",
        "table": "customer_kyc",
    },
    {
        "name": "customer_employment__customer_ref__resolved",
        "constraint": "(customer_ref <> 'UNKNOWN-CUSTOMER')",
        "table": "customer_employment",
    },
    {
        "name": "customer_employment__monthly_income__non_negative",
        "constraint": "(monthly_income >= 0)",
        "table": "customer_employment",
    },
    {
        "name": "customer_request__customer_ref__resolved",
        "constraint": "(customer_ref <> 'UNKNOWN-CUSTOMER')",
        "table": "customer_request",
    },
    {
        "name": "customer_request__description__not_null",
        "constraint": "(description IS NOT NULL)",
        "table": "customer_request",
    },
    {
        "name": "account__cif_number__resolved",
        "constraint": "(cif_number <> 'CIF99999999')",
        "table": "account",
    },
    {
        "name": "customer_account__account_id__resolved",
        "constraint": "(account_id <> -1)",
        "table": "customer_account",
    },
]
