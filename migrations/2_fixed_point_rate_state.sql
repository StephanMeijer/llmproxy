-- One token is 60,000,000 units: each database microsecond adds exactly per_minute units.
-- Preserve existing balances conservatively; incompatible oversized balances fail migration.
-- Decode IEEE-754 directly: float8::numeric rounds significant digits before scaling.
CREATE FUNCTION pg_temp.llmproxy_rate_units(balance double precision) RETURNS numeric
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    bytes bytea := float8send(balance);
    exponent integer := (get_byte(bytes, 0) & 127) * 16 + (get_byte(bytes, 1) >> 4);
    significand numeric := get_byte(bytes, 1) & 15;
BEGIN
    FOR position IN 2..7 LOOP
        significand := significand * 256 + get_byte(bytes, position);
    END LOOP;
    IF exponent = 0 THEN
        exponent := -1074;
    ELSE
        significand := significand + 4503599627370496;
        exponent := exponent - 1075;
    END IF;
    IF exponent >= 0 THEN
        RETURN significand * 60000000 * power(2::numeric, exponent);
    END IF;
    RETURN div(significand * 60000000, power(2::numeric, -exponent));
END;
$$;
ALTER TABLE rate_state DROP CONSTRAINT rate_state_available_check;
ALTER TABLE rate_state ALTER COLUMN available TYPE bigint
    USING pg_temp.llmproxy_rate_units(available)::bigint;
DROP FUNCTION pg_temp.llmproxy_rate_units(double precision);
ALTER TABLE rate_state RENAME COLUMN available TO available_units;
ALTER TABLE rate_state ADD CONSTRAINT rate_state_available_units_check
    CHECK (available_units >= 0 AND available_units <= 515396075400000000);
