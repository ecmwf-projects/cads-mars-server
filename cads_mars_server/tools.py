GLOBE_AREA = 180.0 * 360.0  # degrees² of the full lat/lon domain


def _one_area_fraction(area):
    """Globe fraction covered by one AREA value (N/W/S/E); 1.0 if unparseable."""
    if isinstance(area, str):
        parts = area.split("/")
    elif isinstance(area, (list, tuple)):
        parts = list(area)
    else:
        return 1.0

    if len(parts) != 4:
        return 1.0

    try:
        north, west, south, east = (float(p) for p in parts)
    except (TypeError, ValueError):
        return 1.0

    lat_extent = min(max(north - south, 0.0), 180.0)
    lon_extent = east - west
    if lon_extent <= 0.0:
        # 0/360-crossing selection (e.g. 350/10); a zero width is treated as
        # the full circle — the latitude extent alone then bounds the fraction
        lon_extent += 360.0
    lon_extent = min(lon_extent, 360.0)

    return min(lat_extent * lon_extent / GLOBE_AREA, 1.0)


def area_fraction(request):
    """Fraction of the globe covered by the AREA of *request*.

    *request* is a dict or a list of dicts; the AREA key is matched
    case-insensitively. Returns 1.0 (full allowance) when any request has
    no AREA or the AREA cannot be parsed. When several requests carry an
    AREA, the largest fraction wins — the limit applies to the whole run.
    """
    requests = request if isinstance(request, list) else [request]

    fractions = []
    for req in requests:
        if not isinstance(req, dict):
            return 1.0
        area = next((v for k, v in req.items() if str(k).lower() == "area"), None)
        if area is None:
            return 1.0
        fractions.append(_one_area_fraction(area))

    if not fractions:
        return 1.0
    return max(fractions)


def scaled_max_retrieve_size(request, base=None, floor=None):
    """MARS_MAX_RETRIEVE_SIZE to export for *request*.

    The configured base ceiling is scaled by the AREA fraction and clamped
    to [floor, base].

    The limit the mars executable enforces applies to the post-AREA output,
    so a small-area request can make the backend move full fields worth
    orders of magnitude more data than the output size. Scaling the limit
    by the area fraction keeps the underlying full-field volume bounded by
    roughly the base ceiling. The cds-ansible wrapper only defaults the
    variable (`${MARS_MAX_RETRIEVE_SIZE:=...}`), so the value exported here
    takes precedence.
    """
    from .config import MAX_RETRIEVE_SIZE, MAX_RETRIEVE_SIZE_FLOOR

    if base is None:
        base = MAX_RETRIEVE_SIZE
    if floor is None:
        floor = MAX_RETRIEVE_SIZE_FLOOR

    size = int(base * area_fraction(request))
    return max(min(size, base), min(floor, base))


def bytes(n):
    if n < 0:
        sign = "-"
        n -= 0
    else:
        sign = ""

    u = ["", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB", " YiB"]
    i = 0
    while n >= 1024:
        n /= 1024.0
        i += 1
    return "%s%g%s" % (sign, int(n * 10 + 0.5) / 10.0, u[i])
