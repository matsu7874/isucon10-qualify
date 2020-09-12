DROP TABLE IF EXISTS isuumo._search_chair;

CREATE TABLE isuumo._search_chair AS
    SELECT
        id,
        color,
        features,
        kind,
        popularity,
        stock,
        case
            when price < 3000 then 0
            when price < 6000 then 1
            when price < 9000 then 2
            when price < 12000 then 3
            when price < 15000 then 4
            else 5
        end as price_idx,
        case
            when height < 80 then 0
            when height < 110 then 1
            when height < 150 then 2
            else 3
        end as h_idx,
        case
            when width < 80 then 0
            when width < 110 then 1
            when width < 150 then 2
            else 3
        end as w_idx,
        case
            when depth < 80 then 0
            when depth < 110 then 1
            when depth < 150 then 2
            else 3
        end as d_idx
    FROM
        chair
    WHERE
        stock > 0
;

ALTER TABLE isuumo._search_chair ADD PRIMARY KEY (id);
CREATE INDEX index_search_chair_price_idx ON isuumo._search_chair(price_idx);
CREATE INDEX index_search_chair_h_idx ON isuumo._search_chair(h_idx);
CREATE INDEX index_search_chair_w_idx ON isuumo._search_chair(w_idx);
CREATE INDEX index_search_chair_d_idx ON isuumo._search_chair(d_idx);
