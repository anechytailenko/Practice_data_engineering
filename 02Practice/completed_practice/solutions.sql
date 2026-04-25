# task 1 - not boring cinema leetcode
# Write your MySQL query statement below
SELECT * FROM Cinema 
WHERE id % 2 != 0 AND description != "boring"
ORDER BY rating DESC;

# task 2 - trips leetcode
# Write your MySQL query statement below
WITH Trips AS (
    SELECT 
        t.request_at AS Day,
        t.status,
        CASE 
            WHEN c.banned = 'Yes' OR d.banned = 'Yes' THEN 'Yes' 
            ELSE 'No' 
        END AS trip_banned
    FROM Trips t
    JOIN Users c ON t.client_id = c.users_id
    JOIN Users d ON t.driver_id = d.users_id
)

SELECT Day, ROUND(
        SUM(
            CASE
                WHEN status != 'completed' THEN 1
                ELSE 0
            END
        ) / COUNT(*), 2
    ) AS 'Cancellation Rate'
FROM Trips
WHERE
    trip_banned = 'No'
    AND Day BETWEEN '2013-10-01' AND '2013-10-03'
GROUP BY
    Day;