import httpx


async def get_calories(product_name: str) -> tuple[str | None, float | None, str | None]:
    url = "https://world.openfoodfacts.org/cgi/search.pl"
    params = {
        "search_terms": product_name,
        "search_simple": 1,
        "action": "process",
        "json": 1,
        "page_size": 1,
        "lc": "ru",
    }

    try:
        async with httpx.AsyncClient(timeout = 10) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
    except httpx.RequestError:
        return None, None, "network"
    except httpx.HTTPStatusError:
        return None, None, "network"

    products = data.get("products", [])
    if not products:
        return None, None, "not_found"

    product = products[0]
    name = product.get("product_name_ru") or product.get("product_name")
    kcal = product.get("nutriments", {}).get("energy-kcal_100g")

    if name is None or kcal is None:
        return None, None, "not_found"

    return name, float(kcal), None