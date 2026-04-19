"""Tests for Basis API endpoints — verifies SQL parameter placeholders are correct for PostgreSQL."""


def get_basis_source():
    with open("app/routes/basis.py", "r") as f:
        return f.read()


def test_no_sqlite_placeholders_in_basis_routes():
    """Every parameterised SQL query must use %s (PostgreSQL) not ? (SQLite)."""
    source = get_basis_source()
    lines = source.splitlines()
    violations = []
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if ("execute(" in stripped or "= ?" in stripped) and "?" in stripped and "%s" not in stripped:
            violations.append((i, stripped))

    assert not violations, (
        f"Found {len(violations)} lines still using SQLite '?' placeholders:\n"
        + "\n".join(f"  Line {n}: {t}" for n, t in violations)
    )


def test_helpers_use_correct_placeholders():
    """The paginate helper should use %s for PostgreSQL."""
    with open("app/helpers.py", "r") as f:
        source = f.read()
    assert "%s" in source, "helpers.py should use %s for PostgreSQL"
    for line in source.splitlines():
        if "execute(" in line or "cur.execute" in line:
            assert "?" not in line, f"helpers.py should not use '?' placeholder: {line}"
