select * from takehome.ab_nyc_2019 limit 10;
select count(*) from takehome.ab_nyc_2019;


select count(*) from takehome.ab_nyc_2019;

select count(distinct neighbourhood) from takehome.ab_nyc_2019;

select neighbourhood, sum(calculated_host_listings_count) as count_listings from takehome.ab_nyc_2019
group by 1
order by 2 desc;

select neighbourhood, calculated_host_listings_count
from takehome.ab_nyc_2019
group by 1,2;

select count(distinct neighbourhood) from takehome.ab_nyc_2019;


select neighbourhood, sum(calculated_host_listings_count) as count_listings from takehome.ab_nyc_2019
where neighbourhood = "Hell's Kitchen"
group by 1;



-- 5, 14





