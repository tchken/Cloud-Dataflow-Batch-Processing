
select * from test.products2;


select product_id, max(change_date) from test.products2
where change_date <= '2019-08-16'
group by product_id;

select product_id, new_price from test.products2
where (product_id, change_date) in
(select product_id, max(change_date) as change_date from test.products2
where change_date <= '2019-08-16'
group by 1)







