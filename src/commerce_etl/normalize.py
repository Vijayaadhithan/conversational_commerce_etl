"""Conservative normalization: missing facts remain unknown, listing claims stay claims."""

from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
import math
import re
import unicodedata

from commerce_etl import SCHEMA_VERSION


class TextParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.hidden += 1
        elif tag in {"br", "p", "div", "li"}:
            self.parts.append(" ")

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.hidden = max(0, self.hidden - 1)
        self.parts.append(" ")

    def handle_data(self, value):
        if not self.hidden:
            self.parts.append(value)


def text(value):
    if not isinstance(value, str):
        return None
    parser = TextParser()
    parser.feed(value)
    parser.close()
    result = " ".join(unicodedata.normalize("NFKC", "".join(parser.parts)).split())
    return result or None


def texts(value):
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    return [clean for item in value if (clean := text(item))]


def evidenced_texts(value, field):
    if isinstance(value, str):
        clean = text(value)
        return ([clean], [field]) if clean else ([], [])
    if not isinstance(value, list):
        return [], []
    entries = [(clean, f"{field}/{index}") for index, item in enumerate(value) if (clean := text(item))]
    return [item[0] for item in entries], [item[1] for item in entries]


def price(value):
    if value is None or (isinstance(value, str) and value.strip().lower() in {"", "none", "null", "n/a", "nan"}):
        return None, "missing"
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        return None, "invalid"
    raw = str(value).strip()
    if re.search(r"[£€₹]|\b(?:GBP|EUR|INR|CAD|AUD)\b", raw, re.I):
        return None, "unsupported_currency"
    raw = re.sub(r"^(?:USD\s*|\$\s*)", "", raw, flags=re.I)
    if not re.fullmatch(r"(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d{1,2})?", raw):
        return None, "invalid"
    try:
        amount = Decimal(raw.replace(",", ""))
        if not amount.is_finite() or amount <= 0:
            return None, "invalid"
        return format(amount.quantize(Decimal("0.01")), "f"), "valid"
    except InvalidOperation:
        return None, "invalid"


# Rules classify broad families, not validated suitability or purchasable variants.
# Category-path evidence takes precedence over title rules. Unmatched items stay unknown.
CATEGORY_RULES = [
    ("beauty", "skin_care", r"\bskin care\b"),
    ("beauty", "hair_care", r"\bhair care\b|\bwigs\b|\bhair extensions\b"),
    ("beauty", "makeup", r"\bmakeup\b|\bmake-up\b|\bnail polish\b"),
    ("beauty", "fragrance", r"\bfragrances?\b|\bperfumes?\b|\bcolognes?\b"),
    ("beauty", "oral_care", r"\boral care\b|\btoothbrushes?\b|\btoothpaste\b"),
    ("beauty", "shaving", r"\bshaving\b|\bhair removal\b|\brazors?\b"),
    ("beauty", "beauty_tools", r"\bbeauty tools\b|\bmakeup brushes\b"),
    ("beauty", "personal_care", r"\bdeodorants?\b|\bantiperspirants?\b|\bbath(?:ing)? accessories\b"),
    ("fashion", "footwear", r"\bshoes\b|\bfootwear\b|\bboots\b|\bsandals\b"),
    ("fashion", "bags", r"\bhandbags\b|\bbackpacks\b|\bluggage\b|\bwallets\b"),
    ("fashion", "jewelry", r"\bjewelry\b|\bjewellery\b"),
    ("fashion", "watches", r"\bwatches\b"),
    ("fashion", "apparel", r"\bclothing\b(?! accessories)|\bapparel\b|\bsocks\b"),
    ("fashion", "accessories", r"\bclothing accessories\b|\bsunglasses\b|\bbelts\b|\bscarves\b"),
]
TITLE_RULES = [
    ("beauty", "skin_care", r"\bmoisturi[sz]er\b|\b(?:face|facial|body|hand|skin) (?:cream|lotion|cleanser|serum|mask|wash)\b|\bsunscreen\b"),
    ("beauty", "hair_care", r"\bshampoo\b|\bhair (?:conditioner|color|colour|dye|chalk|spray|oil|extensions?|gel)\b|\bwigs?\b"),
    ("beauty", "makeup", r"\blipstick\b|\blip gloss\b|\bmascara\b|\beyeliner\b|\beye ?shadow\b|\bmakeup foundation\b|\bnail polish\b"),
    ("beauty", "fragrance", r"\bperfume\b|\beau de (?:parfum|toilette)\b|\bcologne\b"),
    ("beauty", "oral_care", r"\btoothbrush\b|\btoothpaste\b|\bdental floss\b|\bmouthwash\b"),
    ("beauty", "shaving", r"\bshaving\b|\brazor\b|\bepilator\b"),
    ("beauty", "beauty_tools", r"\bmakeup brush\b|\beyelash curler\b|\bbeauty sponge\b"),
    ("beauty", "personal_care", r"\bdeodorant\b|\bantiperspirant\b|\bbody wash\b|\bhand soap\b"),
    ("fashion", "footwear", r"\bshoes?\b|\bsneakers?\b|\bsandals?\b|\bboots?\b|\bslippers?\b"),
    ("fashion", "bags", r"\bhandbags?\b|\bbackpacks?\b|\bwallets?\b|\bshoulder bags?\b|\btote bags?\b|\bcrossbody\b"),
    ("fashion", "jewelry", r"\bearrings?\b|\bnecklaces?\b|\bbracelets?\b|\bpendants?\b|\bwedding bands?\b"),
    ("fashion", "watches", r"\bwrist ?watch(?:es)?\b|\b(?:men'?s|women'?s) watch(?:es)?\b"),
    ("fashion", "apparel", r"\bt-?shirts?\b|\bshirts?\b|\bblouses?\b|\bdresses?\b|\bpants\b|\bjeans\b|\bsocks\b|\bhoodies?\b|\bjackets?\b|\bsweaters?\b|\bskirts?\b|\bleggings\b|\bunderwear\b|\bbras?\b|\bswimwear\b"),
    ("fashion", "accessories", r"\bsunglasses\b|\bscar(?:f|ves)\b|\bbaseball caps?\b|\bwinter hats?\b|\b(?:leather|waist) belts?\b"),
]
CATEGORY_RULES = [(d, f, re.compile(p, re.I)) for d, f, p in CATEGORY_RULES]
TITLE_RULES = [(d, f, re.compile(p, re.I)) for d, f, p in TITLE_RULES]
EXCLUSIONS = re.compile(r"\bleather (?:conditioner|cleaner)\b|\bshoe polish\b|\bcar wax\b|\bdog shampoo\b|\bpet shampoo\b", re.I)


def classify(title, categories, category_paths=None):
    if EXCLUSIONS.search(title):
        return {"domain": "unknown", "family": "unknown", "basis": "out_of_scope_rule", "evidence": None}
    for index in reversed(range(len(categories))):
        category = categories[index]
        matches = {(domain, family) for domain, family, pattern in CATEGORY_RULES if pattern.search(category)}
        if len(matches) == 1:
            domain, family = matches.pop()
            return {"domain": domain, "family": family, "basis": "category_rule",
                    "evidence": {"field": category_paths[index] if category_paths else f"/categories/{index}", "text": category}}
    matches = {(domain, family) for domain, family, pattern in TITLE_RULES if pattern.search(title)}
    if len(matches) == 1:
        domain, family = matches.pop()
        return {"domain": domain, "family": family, "basis": "title_rule",
                "evidence": {"field": "/title", "text": title}}
    return {"domain": "unknown", "family": "unknown", "basis": None, "evidence": None}


DETAIL_FIELDS = {
    "color": ("Color", "Colour"), "size": ("Size",),
    "material": ("Material", "Fabric Type"), "department": ("Department",),
    "age_range": ("Age Range (Description)",), "skin_type": ("Skin Type",),
    "hair_type": ("Hair Type",), "ingredients": ("Ingredients",),
    "active_ingredients": ("Active Ingredients",), "product_benefits": ("Product Benefits",),
    "material_type_free": ("Material Type Free",), "scent": ("Scent",),
    "item_form": ("Item Form",), "unit_count": ("Unit Count",),
}


def detail_claim(details, keys):
    for key in keys:
        if value := text(details.get(key)):
            return {"value": value, "field": "/details/" + key.replace("~", "~0").replace("/", "~1"),
                    "status": "listing_claim"}
    return None


def normalize(record, source, row, record_hash):
    if not isinstance(record, dict):
        raise ValueError("not_an_object")
    asin = record.get("parent_asin")
    if not isinstance(asin, str) or not re.fullmatch(r"[A-Z0-9]{10}", asin):
        raise ValueError("invalid_product_id")
    title = text(record.get("title"))
    if not title:
        raise ValueError("missing_title")
    details = record.get("details") if isinstance(record.get("details"), dict) else {}
    categories, category_paths = evidenced_texts(record.get("categories"), "/categories")
    category = classify(title, categories, category_paths)
    amount, price_status = price(record.get("price"))
    attributes = {name: detail_claim(details, keys) for name, keys in DETAIL_FIELDS.items()}
    brand = detail_claim(details, ("Brand",))
    # Unscented is intentionally not converted into fragrance-free.
    fragrance_free = None
    for name in ("material_type_free", "scent"):
        claim = attributes[name]
        if claim and re.fullmatch(r"fragrance[ -]free", claim["value"], re.I):
            fragrance_free = {**claim, "value": True}
            break
    attributes["fragrance_free"] = fragrance_free
    description, description_fields = evidenced_texts(record.get("description"), "/description")
    features, feature_fields = evidenced_texts(record.get("features"), "/features")
    images = []
    if isinstance(record.get("images"), list):
        for image in record["images"]:
            if isinstance(image, dict):
                url = image.get("hi_res") or image.get("large") or image.get("thumb")
                if isinstance(url, str) and re.match(r"^https?://[^\s]+$", url) and url not in images:
                    images.append(url)
    rating = record.get("average_rating")
    if isinstance(rating, bool) or not isinstance(rating, (int, float)) or not math.isfinite(rating) or not 0 <= rating <= 5:
        rating = None
    rating_count = record.get("rating_number")
    if isinstance(rating_count, bool) or not isinstance(rating_count, int) or rating_count < 0:
        rating_count = None
    warnings = []
    if category["family"] == "unknown": warnings.append("unclassified")
    if category["basis"] == "title_rule": warnings.append("category_inferred_from_title")
    if amount is None: warnings.append("price_" + price_status)
    if not description and not features: warnings.append("no_descriptive_content")
    quality_score = (int(category["basis"] == "category_rule") * 100 + int(amount is not None) * 30
                     + int(bool(description or features)) * 20 + sum(v is not None for v in attributes.values())
                     + int(brand is not None) * 5)
    product = {
        "schema_version": SCHEMA_VERSION, "product_id": "amazon:" + asin,
        "parent_asin": asin, "title": title, "category": category,
        "brand": brand, "store": text(record.get("store")),
        "price": {"amount": amount, "currency": "USD" if amount is not None else None,
                  "status": "historical" if amount is not None else "unknown", "parse_status": price_status,
                  "field": "/price", "observed_value": record.get("price")},
        "categories": categories, "attributes": attributes,
        "content": {"description": description, "features": features},
        "content_source_fields": {"description": description_fields, "features": feature_fields},
        "image_urls": images, "rating": {"average": rating, "count": rating_count},
        "availability": None,
        "selected_observation": {"source": source, "row": row, "record_sha256": record_hash},
        "quality": {"score": quality_score, "warnings": warnings, "conflicting_fields": []},
    }
    product["quality"]["eligible_for_subset"] = eligible(product, [])
    return product


def conflict_values(product):
    values = {"title": product["title"], "price": product["price"]["amount"],
              "family": product["category"]["family"] if product["category"]["family"] != "unknown" else None,
              "brand": product["brand"]["value"] if product["brand"] else None}
    values.update({key: product["attributes"][key]["value"] if product["attributes"][key] else None
                   for key in ("color", "size", "material")})
    return values


def eligible(product, conflicts):
    return (product["category"]["domain"] != "unknown" and product["price"]["amount"] is not None
            and bool(product["content"]["description"] or product["content"]["features"])
            and not set(conflicts).intersection({"price", "family", "brand", "color", "size", "material"}))
