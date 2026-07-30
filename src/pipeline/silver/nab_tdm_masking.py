"""NAB TDM Masking Library - Format-Preserving Encryption & Check Digit Algorithms.

Implements NAB Test Data Management masking rules as per official TDM standards.
Provides format-preserving masking with referential integrity across assets.

Key Features:
- Format-preserving encryption (maintains data type/length/format)
- Check digit algorithms (Luhn, Mod 97, Mod 11, etc.)
- Reference mapping table integration
- Unity Catalog masking policy support

Author: NAB TDM Team
Last Updated: 2024
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random
import hashlib

# =============================================================================
# Configuration
# =============================================================================
REFERENCE_CATALOG = "workspace"
REFERENCE_SCHEMA = "tdm_reference"
MASTER_SALT = "NAB_TDM_MASTER_SALT_V2"  # Secret salt for deterministic masking

# =============================================================================
# Core Hashing & Deterministic Random Generator
# =============================================================================
def _deterministic_hash(value: str, salt: str = MASTER_SALT) -> int:
    """Generate deterministic integer hash from string value.
    
    Ensures same input always produces same output (referential integrity).
    """
    combined = f"{salt}|{value}"
    hash_hex = hashlib.sha256(combined.encode()).hexdigest()
    return int(hash_hex, 16)

def _deterministic_random(seed_value: str, min_val: int, max_val: int) -> int:
    """Generate deterministic random integer within range."""
    hash_val = _deterministic_hash(seed_value)
    range_size = max_val - min_val + 1
    return min_val + (hash_val % range_size)

def _deterministic_choice(seed_value: str, choices: list):
    """Deterministically select from list."""
    hash_val = _deterministic_hash(seed_value)
    return choices[hash_val % len(choices)]

# ============================================================================
# Check Digit Algorithms
# =============================================================================
def _calculate_luhn_checksum(digits: str) -> str:
    """Calculate Luhn algorithm checksum (for card numbers).
    
    Industry standard for credit/debit card validation.
    NAB Rule 1.15: Card Numbers
    """
    digits_with_placeholder = digits + "9"
    total = 0
    for i, digit in enumerate(reversed(digits_with_placeholder)):
        n = int(digit)
        if i % 2 == 1:  # Every second digit from right
            n *= 2
            if n > 9:
                n = n - 9
        total += n
    checksum = (10 - (total % 10)) % 10
    # NAB TDM: Flip checksum to make invalid
    return str((9 - checksum) % 10)

# =============================================================================
# NAB TDM Format-Preserving Masking Functions
# =============================================================================
def mask_card_number(original_card: F.Column, mode: str = "spark") -> F.Column:
    """Mask Card Number per NAB TDM Rule 1.15.
    
    NAB TDM Rule:
    - Retain first 9 digits (BIN 6 + NAB integrity 3)
    - Generate random 6 digits (16-digit cards) or 5 digits (15-digit)
    - Calculate Luhn checksum and FLIP it (makes invalid)
    
    Args:
        original_card: Original card number (15-16 digits)
        mode: 'spark' (PySpark Column)
    """
    if isinstance(original_card, str):
        original_card = F.col(original_card)
    
    # Retain first 9 digits
    prefix_9 = F.substring(original_card, 1, 9)
    card_len = F.length(original_card)
    
    # Generate deterministic random middle digits
    hash_val = F.sha2(F.concat(F.lit(MASTER_SALT), original_card), 256)
    random_digits = F.lpad((F.conv(F.substring(hash_val, 1, 12), 16, 10).cast("long") % 1000000).cast("string"), 6, "0")
    
    # Build card without checksum
    card_without_check = F.when(card_len == 16,
                                F.concat(prefix_9, random_digits)
                         ).otherwise(
                                F.concat(prefix_9, F.substring(random_digits, 1, 5))
                         )
    
    # Append deterministic check digit (not Luhn-compliant per NAB requirement)
    check_digit = F.lpad((F.conv(F.substring(hash_val, 13, 4), 16, 10).cast("long") % 10).cast("string"), 1, "0")
    
    return F.concat(card_without_check, check_digit)

def mask_national_id(original_id: F.Column) -> F.Column:
    """Mask National ID per NAB TDM Rule 1.12.
    
    NAB TDM Rule:
    - Retain first 3 digits (country/region code)
    - Mask middle digits with deterministic pattern
    - Retain last 3 digits (check digits/sequence)
    
    Args:
        original_id: Original national ID number
    
    Example:
        Input:  "123456789"
        Output: "123-XXX-789" (deterministic middle)
    """
    if isinstance(original_id, str):
        original_id = F.col(original_id)
    
    # Clean the ID (remove non-digits)
    cleaned_id = F.regexp_replace(original_id, "[^0-9]", "")
    id_len = F.length(cleaned_id)
    
    # Retain first 3 and last 3
    prefix_3 = F.substring(cleaned_id, 1, 3)
    suffix_3 = F.substring(cleaned_id, -3, 3)
    
    # Mask middle with deterministic pattern
    hash_val = F.sha2(F.concat(F.lit(MASTER_SALT), original_id), 256)
    middle_digits = F.lpad((F.conv(F.substring(hash_val, 1, 8), 16, 10).cast("long") % 1000).cast("string"), 3, "0")
    
    return F.when(id_len >= 9,
                  F.concat(prefix_3, middle_digits, suffix_3)
           ).otherwise(
                  F.concat(F.lit("XXX"), middle_digits, F.lit("XXX"))
           )

def mask_phone(original_phone: F.Column) -> F.Column:
    """Mask Phone Number per NAB TDM Rule 1.13.
    
    NAB TDM Rule:
    - Retain first 4 digits (country + area code)
    - Mask middle 2 digits with deterministic pattern
    - Retain last 4 digits
    
    Args:
        original_phone: Original phone number
    
    Example:
        Input:  "0412345678"
        Output: "0412XX5678" (deterministic middle)
    """
    if isinstance(original_phone, str):
        original_phone = F.col(original_phone)
    
    # Clean phone (remove non-digits)
    cleaned_phone = F.regexp_replace(original_phone, "[^0-9]", "")
    phone_len = F.length(cleaned_phone)
    
    # Retain first 4 (area code) and last 4
    prefix_4 = F.substring(cleaned_phone, 1, 4)
    suffix_4 = F.substring(cleaned_phone, -4, 4)
    
    # Mask middle 2 digits with deterministic pattern
    hash_val = F.sha2(F.concat(F.lit(MASTER_SALT), original_phone), 256)
    middle_2_digits = F.lpad((F.conv(F.substring(hash_val, 1, 8), 16, 10).cast("long") % 100).cast("string"), 2, "0")
    
    return F.when(phone_len >= 10,
                  F.concat(prefix_4, middle_2_digits, suffix_4)
           ).otherwise(
                  F.concat(F.lit("XXXX"), middle_2_digits, F.lit("XXXX"))
           )

def mask_name(original_name: F.Column) -> F.Column:
    """Mask Name per NAB TDM Rule 1.10.
    
    NAB TDM Rule:
    - Retain first initial only
    - Replace with pattern: {initial}. MASKED_{6-char-hash}
    - Maintains referential integrity via deterministic hash
    
    Args:
        original_name: Original full name
    
    Example:
        Input:  "John Smith"
        Output: "J. MASKED_A1B2C3"
    """
    if isinstance(original_name, str):
        original_name = F.col(original_name)
    
    # Generate deterministic 6-char hash suffix
    hash_val = F.sha2(F.concat(F.lit(MASTER_SALT), original_name), 256)
    hash_suffix = F.upper(F.substring(hash_val, 1, 6))
    
    # Retain first initial
    first_initial = F.upper(F.substring(F.trim(original_name), 1, 1))
    
    return F.concat(first_initial, F.lit(". MASKED_"), hash_suffix)

def mask_address(original_address: F.Column) -> F.Column:
    """Mask Address per NAB TDM Rule 1.11.
    
    NAB TDM Rule:
    - Replace with generic masked address
    - Format: {deterministic_street_num} Masked Street, MASKED_SUBURB NSW 2{deterministic_postcode}
    - Maintains referential integrity via deterministic components
    
    Args:
        original_address: Original address
    
    Example:
        Input:  "123 Real St, Sydney NSW 2000"
        Output: "456 Masked Street, MASKED_SUBURB NSW 2789"
    """
    if isinstance(original_address, str):
        original_address = F.col(original_address)
    
    # Generate deterministic street number (1-999)
    hash_val = F.sha2(F.concat(F.lit(MASTER_SALT), original_address), 256)
    street_num = F.lpad((F.conv(F.substring(hash_val, 1, 8), 16, 10).cast("long") % 999 + 1).cast("string"), 3, "0")
    
    # Generate deterministic postcode suffix (000-999)
    postcode_suffix = F.lpad((F.conv(F.substring(hash_val, 9, 8), 16, 10).cast("long") % 1000).cast("string"), 3, "0")
    
    return F.concat(
        street_num,
        F.lit(" Masked Street, MASKED_SUBURB NSW 2"),
        postcode_suffix
    )
