"""Domain-routed Lakeflow quality rules.

Each domain module owns a data-only ``RULES`` inventory. This module is the
single public API and implementation point for rule lookup and quarantine
predicate construction, so pipelines have no helper-module dependency chain.
"""

import card_quality_rules
import customer_quality_rules
import fincrime_quality_rules
import transaction_quality_rules


DOMAIN_MODULES = {
    "card": card_quality_rules,
    "customer": customer_quality_rules,
    "transaction": transaction_quality_rules,
    "fincrime": fincrime_quality_rules,
}

DOMAIN_RULES = {
    domain: tuple(module.RULES)
    for domain, module in DOMAIN_MODULES.items()
}

RULES_BY_TABLE = {}
for _domain_rules in DOMAIN_RULES.values():
    for _rule in _domain_rules:
        RULES_BY_TABLE.setdefault(_rule["table"], []).append(_rule)


def get_rules_as_list_of_dict():
    """Return the complete, domain-split rule inventory."""

    return [
        rule
        for rules in DOMAIN_RULES.values()
        for rule in rules
    ]


def get_domain_rules(domain):
    """Return a domain's inventory, rejecting unknown domain names."""

    try:
        return list(DOMAIN_RULES[domain])
    except KeyError as error:
        raise ValueError(f"No data-quality rules found for domain: {domain!r}") from error


def get_rules(table):
    """Return Lakeflow expectations for one table in any supported domain."""

    try:
        return {
            rule["name"]: rule["constraint"]
            for rule in RULES_BY_TABLE[table]
        }
    except KeyError as error:
        raise ValueError(f"No data-quality rules found for table: {table!r}") from error


def get_quarantine_condition(table):
    """Return the predicate that identifies rows for quarantine."""

    return "NOT({0})".format(" AND ".join(get_rules(table).values()))
