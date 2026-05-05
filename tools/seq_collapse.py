def split_token(tok: str) -> tuple[str, str]:
    head, tail = tok.split(".", 1)
    return head, tail

def collapse_naive(s):
    """
    Regimen string processing includes:
    1. Find shortest repeating prefix that reconstructs the full string
    2. If collapsed output has only one token → duplicate it (min-2-rule)
    3. Lowercase + trailing semicolon
    """
    s = s.lower().strip()
    if not s.endswith(";"):
        s += ";"

    tokens = [t for t in s.split(";") if t]
    n = len(tokens)

    
    filtered = []
    for tok in tokens:
        head, drug = split_token(tok)
        if head == "0":
            continue
        filtered.append(tok)
    tokens = filtered
    n = len(tokens)

    # Find how many times chunk can be repeated in full list of tokens
    for size in range(1, n + 1):
        chunk = tokens[:size]
        if chunk * (n // size) == tokens[:size * (n // size)] and n % size == 0:
            collapsed = chunk
            break

    # Enforce min-two-token rule
    if len(collapsed) == 1:
        collapsed = collapsed * 2

    return ";".join(collapsed) + ";" 


def filter_et(s: str) -> str:
    """
    Filter out entries with 0 positions and resolve multi-cycle information to 1D.
    
    Input example: 
        "7.daratumumab@len15;0.daratumumab@len22;0.daratumumab@len28;7.daratumumab@len22;7.daratumumab@len15;0.daratumumab@len22;7.daratumumab@len22"
    
    Output example:
        "7.daratumumab;7.daratumumab;7.daratumumab;7.daratumumab;"
    
    Logic:
    - Discard all parts starting with "0." (same drug same day)
    - Keep only <days>.<drug-name> part (remove everything after "@")
    - Add trailing semicolon for proper processing
    """
    if not s:
        return s
    
    # Split by semicolon
    parts = s.split(";")
    
    # Filter and clean each part
    filtered = []
    for part in parts:
        # Skip empty parts
        if not part:
            continue
        
        # Skip parts starting with "0."
        if part.startswith("0."):
            continue
        
        # Remove everything after "@" (dosage information like @len15)
        cleaned = part.split("@")[0]
        filtered.append(cleaned)
    
    # Join back with semicolon and add trailing semicolon
    return ";".join(filtered)