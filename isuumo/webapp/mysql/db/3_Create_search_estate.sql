DROP TABLE IF EXISTS isuumo._search_estate;

CREATE TABLE isuumo._search_estate AS
    SELECT
        id,
        latitude,
        longitude,
        rent,
        door_height,
        door_width,
        features,
        popularity,
        case
            when rent < 50000 then 0
            when rent < 100000 then 1
            when rent < 150000 then 2
            else 3
        end as rent_idx,
        case
            when door_height < 80 then 0
            when door_height < 110 then 1
            when door_height < 150 then 2
            else 3
        end as dh_idx,
        case
            when door_width < 80 then 0
            when door_width < 110 then 1
            when door_width < 150 then 2
            else 3
        end as dw_idx
    FROM
        estate
;

ALTER TABLE isuumo._search_estate ADD PRIMARY KEY (id);
CREATE INDEX index_rent_idx ON isuumo._search_estate(rent_idx);
CREATE INDEX index_dh_idx ON isuumo._search_estate(dh_idx);
CREATE INDEX index_dw_idx ON isuumo._search_estate(dw_idx);
CREATE INDEX index_features ON isuumo._search_estate(features);
