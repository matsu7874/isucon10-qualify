CREATE TABLE isuumo._search_estate AS
    SELECT
        id,
        latitude,
        longitude,
        features,
        popularity,
        case
            when rent < 50000 then 0
            when rent < 100000 then 1
            when rent < 150000 then 2
            else 3
        end case as rent_idx,
        case
            when door_height < 80 then 0
            when door_height < 110 then 1
            when door_height < 150 then 2
            else 3
        end case as dh_idx,
        case
            when door_width < 80 then 0
            when door_width < 110 then 1
            when door_width < 150 then 2
            else 3
        end case as dw_idx
    FROM
        estate
;

ALTER TABLE isuumo._search_estate ADD PRIMARY KEY id;
CREATE INDEX index_rent_idx ON isuumo._search_estate(rent_idx);
CREATE INDEX index_dh_idx ON isuumo._search_estate(dh_idx);
CREATE INDEX index_dw_idx ON isuumo._search_estate(dw_idx);
CREATE INDEX index_popularity ON isuumo._search_estate(popularity);
