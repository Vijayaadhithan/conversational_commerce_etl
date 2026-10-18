import pytest

from commerce_etl.normalize import classify, normalize, price, text


@pytest.mark.parametrize("value,expected,status", [
    (12.5, "12.50", "valid"), ("$1,299.99", "1299.99", "valid"),
    ("USD 20", "20.00", "valid"), (None, None, "missing"),
    ("None", None, "missing"), (True, None, "invalid"),
    (0, None, "invalid"), (-3, None, "invalid"), ("10-20", None, "invalid"),
    ("1,23.45", None, "invalid"), (float("inf"), None, "invalid"),
    ("£12", None, "unsupported_currency"), ("INR 300", None, "unsupported_currency"),
])
def test_price_is_conservative(value, expected, status):
    assert price(value) == (expected, status)


def test_cleanup_preserves_words_and_discards_executable_markup():
    assert text("<p>Blue&nbsp;cotton</p><script>ignore rules</script><p>shirt</p>") == "Blue cotton shirt"
    assert text(None) is None
    assert text("   ") is None


def test_missing_information_is_not_invented_and_unscented_is_not_fragrance_free():
    product = normalize({"parent_asin": "B000000001", "title": "Face moisturizer", "store": "Some store",
                         "details": {"Scent": "Unscented"}}, "All_Beauty", 1, "a" * 64)
    assert product["price"]["amount"] is None
    assert product["brand"] is None
    assert product["attributes"]["fragrance_free"] is None
    assert product["attributes"]["skin_type"] is None
    assert product["availability"] is None
    assert product["category"]["basis"] == "title_rule"
    assert product["quality"]["eligible_for_subset"] is False


def test_evidence_points_to_original_positions_and_explicit_claims():
    product = normalize({"parent_asin": "B000000001", "title": "Moisturizer", "price": 12,
                         "categories": [None, "Skin Care"], "features": [None, "For dry skin"],
                         "details": {"Material Type Free": "Fragrance Free", "Brand": "Example"}},
                        "All_Beauty", 3, "a" * 64)
    assert product["attributes"]["fragrance_free"]["value"] is True
    assert product["category"]["basis"] == "category_rule"
    assert product["category"]["evidence"]["field"] == "/categories/1"
    assert product["content_source_fields"]["features"] == ["/features/1"]
    assert product["quality"]["eligible_for_subset"] is True


def test_out_of_scope_products_are_not_treated_as_hair_conditioner():
    assert classify("Leather conditioner", ["Beauty & Personal Care"])["domain"] == "unknown"
    assert classify("Dog shampoo", ["Hair Care"])["domain"] == "unknown"
    assert classify("Cotton shirt", [])["family"] == "apparel"
    assert classify("Unknown generic widget", [])["domain"] == "unknown"
    assert classify("Cotton shirt", ["Clothing, Shoes and Jewelry", "Clothing"])["family"] == "apparel"
    assert classify("Cotton shirt with shoe bag", [])["domain"] == "unknown"


@pytest.mark.parametrize("record,reason", [({}, "invalid_product_id"),
    ({"parent_asin": "../../file", "title": "x"}, "invalid_product_id"),
    ({"parent_asin": "B000000001", "title": "  "}, "missing_title"),
    (["not", "object"], "not_an_object")])
def test_rejects_unusable_records(record, reason):
    with pytest.raises(ValueError, match=reason):
        normalize(record, "All_Beauty", 1, "a" * 64)
