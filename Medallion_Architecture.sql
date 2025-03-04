create database Bronze

----------->BRONZE<---------------------------

create table bronze.dbo.stores(store_id int primary key,store_name varchar(20),ph_no bigint,store_address varchar(20))

insert into stores values(1210,'fashion world',6789424383,'chennai')
insert into stores values(1211,'instore',8902424383,'chennai')
insert into stores values(1212,'diadem',9789424383,'madurai')
insert into stores values(1213,'trends',6789424383,'Chennai')
insert into stores values(1214,'maharaj',8689424390,'trichy')
select * from stores

create table bronze.dbo.staffs(staff_id int,staff_name varchar(20),ph_num bigint,active varchar(20),
                    store_id int foreign key references stores(store_id));

insert into staffs values(101,'yuvani',9087654320,'yes',1210)
insert into staffs values(102,'vino',9787854389,'yes',1211)
insert into staffs values(103,'madhu',8087326320,'yes',1212)
insert into staffs values(104,'ravi',7887654501,'yes',1213)
insert into staffs values(105,'ram',6347654320,'yes',1214)
select * from staffs

create table bronze.dbo.categories(category_id int primary key,category_name varchar(20),store_id int references stores(store_id))

insert into categories values(2310,'mensclothing',1210)
insert into categories values(2311,'womens clothing',1211)
insert into categories values(2312,'kids clothing',1212)
insert into categories values(2313,'sports wear',1213)
insert into categories values(2314,'winter wear',1214)
select  * from categories
use bronze
create table bronze.dbo.products(prod_id int primary key,product_name varchar(30),store_id int foreign key references stores(store_id),price int)

insert into products values(2011,'shirt',1210,900)
insert into products values(2012,'kurtis',1211,1500)
insert into products values(2013,'jumpsuit',1212,1000)
insert into products values(2014,'shorts',1213,700)
insert into products values(2015,'hoodie',1214,2000)


create table bronze.dbo.orders(order_id int,
                    store_id int foreign key references stores(store_id),
					order_date date,
					prod_id int foreign key references products(prod_id),
					orders int
					)

insert into orders values(1111,1210,'2024-03-23',2011,5)
insert into orders values(1112,1211,'2024-04-03',2012,8)
insert into orders values(1113,1212,'2024-02-13',2013,10)
insert into orders values(1114,1213,'2024-03-15',2014,6)
insert into orders values(1115,1214,'2024-01-05',2015,3)
select * from bronze.dbo.orders

create table bronze.dbo.customers(customer_id int primary key,c_name varchar(30),
                       ph_number bigint,c_address varchar(30),store_id int references stores(store_id),
					   statuss varchar(20)); 
					  

insert into customers values(1001,'Harshana',8978965433,'Royapettah',1210,'delivered')
insert into customers values(1002,'Anitha',8178342439,'Nungampakkam',1211,'not delivered')
insert into customers values(1004,'Arun',7978965483,'Ramnad',1212,'delivered')
insert into customers values(1005,'kavi',9978235897,'Pudukottai',1213,'not delivered')
insert into customers values(1006,'Vinu',6378305018,'Trichy',1214,'not delivered')

select * from bronze.dbo.customers






select * from stores
select * from staffs
select * from categories
select * from products
select * from orders
select * from customers

--------------->silver<------------------------------------

create database Silver
use silver

create table silver.dbo.stores(store_id int primary key,store_name varchar(20),ph_no bigint,store_address varchar(20),statuss varchar(20),updated_date date)

create table silver.dbo.staffs(staff_id int,staff_name varchar(20),ph_num bigint,active varchar(20),
                    store_id int foreign key references stores(store_id),
					statuss varchar(20),updated_date date);

create table silver.dbo.categories(category_id int primary key,category_name varchar(20),
                        store_id int references stores(store_id),
                        statuss varchar(20),updated_date date)

create table silver.dbo.products(prod_id int primary key,product_name varchar(30),
                                 store_id int foreign key references stores(store_id),price int,
                                 statuss varchar(20),updated_date date)
								 
create table silver.dbo.orders(order_id int,
                    store_id int foreign key references stores(store_id),
					order_date date,
					prod_id int foreign key references products(prod_id),
					orders int,
					statuss varchar(20),updated_date date,
					)

create table silver.dbo.customers(customer_id int primary key,c_name varchar(30),
                       ph_number bigint,c_address varchar(30),store_id int references stores(store_id),
					   statuss varchar(20),updated_date date); 

					   drop table customers

-------------->stores<---------------------
merge into silver.dbo.stores as target
using bronze.dbo.stores as source
on target.store_id=source.store_id
when matched then
update set target.store_name=source.store_name,
           target.ph_no=source.ph_no,target.store_address=source.store_address,
		   target.statuss='updated',target.updated_date=getdate()
when not matched then
insert(store_id,store_name,ph_no,store_address,statuss,updated_date)values
      (source.store_id,source.store_name,source.ph_no,source.store_address,'inserted',getdate());           

select * from stores

-------------->staffs<--------------------------

merge into silver.dbo.staffs as target
using bronze.dbo.staffs as source
on target.staff_id=source.staff_id
when matched then
update set target.staff_name=source.staff_name,
           target.ph_num=source.ph_num,target.active=source.active,
		   target.store_id=source.store_id,target.statuss='updated',target.updated_date=getdate()
when not matched then
insert(staff_id,staff_name,ph_num,active,store_id,statuss,updated_date)values
      (source.staff_id,source.staff_name,source.ph_num,source.active,source.store_id,'inserted',getdate());           

select *from staffs

---------------->categories<---------------------
merge into Silver.dbo.categories as target
using Bronze.dbo.categories as source
on target.category_id=source.category_id
when matched then
update set target.category_name=source.category_name,target.store_id=source.store_id,target.statuss='updated',target.updated_date=getdate()
when not matched then
insert (category_id,category_name,store_id,statuss,updated_date)values(source.category_id,source.category_name,source.store_id,'inserted',getdate());

select *from categories

------------->products<--------------------
merge into Silver.dbo.products as target
using Bronze.dbo.products as source
on target.prod_id=source.prod_id
when matched then
update set target.product_name=source.product_name,target.store_id=source.store_id,target.price=source.price,target.statuss='updated',target.updated_date=getdate()
when not matched then
insert (prod_id,product_name,store_id,price,statuss,updated_date)values(source.prod_id,source.product_name,source.store_id,source.price,'inserted',getdate());
select * from products

-------------------->orders<----------------------
merge into silver.dbo.orders as target
using bronze.dbo.orders as source
on target.order_id=source.order_id
when matched then
update set target.store_id=source.store_id,
           target.order_date=source.order_date,
		   target.prod_id=source.prod_id,
		   target.orders=source.orders,
		   target.statuss='updated',target.updated_date=getdate()
		 
when not matched then
insert(order_id,store_id,order_date,prod_id,orders,statuss,updated_date)values
      (source.order_id,source.store_id,source.order_date,source.prod_id,source.orders,'inserted',getdate());           


select * from bronze.dbo.orders

drop table silver.dbo.orders


---------------------->customers<--------------------

merge into Silver.dbo.customers as target
using Bronze.dbo.customers as source
on target.customer_id=source.customer_id
when matched then
update set target.c_name=source.c_name,
           target.ph_number=source.ph_number,
		   target.c_address=source.c_address,
		   target.store_id=source.store_id,
		   target.statuss=source.statuss,target.updated_date=getdate()
when not matched then
insert (customer_id,c_name,ph_number,c_address,store_id,statuss,updated_date)values
       (source.customer_id,source.c_name,source.ph_number,source.c_address,source.store_id,source.statuss,getdate());

select * from customers

--------------> Gold <-------------------

create database Gold

use Gold

select silver.dbo.stores.store_name,
       silver.dbo.stores.ph_no,
	   silver.dbo.staffs.staff_id,
	   silver.dbo.categories.category_name,
	   silver.dbo.products.product_name,
	   silver.dbo.orders.order_id,
	   silver.dbo.customers.customer_id,
	   silver.dbo.customers.statuss

FROM 
    silver.dbo.stores
JOIN 
    silver.dbo.staffs ON stores.store_id = staffs.store_id
JOIN 
    silver.dbo.products ON stores.store_id = products.store_id
JOIN 
    silver.dbo.categories ON products.store_id = categories.store_id
JOIN 
    silver.dbo.orders ON products.store_id = orders.store_id
JOIN 
    silver.dbo.customers ON orders.store_id = customers.store_id;


INSERT INTO gold.dbo.retail (
    store_name, 
    ph_no, 
    staff_id, 
    category_name, 
    product_name, 
    order_id, 
    prod_id, 
    price, 
    orders, 
    customer_id, 
    statuss, 
    updated_date
)
SELECT
    stores.store_name,
    stores.ph_no,
    staffs.staff_id,
    categories.category_name,
    products.product_name,
    orders.order_id,
    orders.prod_id,
    products.price,
    orders.orders,
    customers.customer_id,
    customers.statuss,
    customers.updated_date
FROM 
    silver.dbo.stores
JOIN 
    silver.dbo.staffs ON stores.store_id = staffs.store_id
JOIN 
    silver.dbo.products ON stores.store_id = products.store_id
JOIN 
    silver.dbo.categories ON products.store_id = categories.store_id
JOIN 
    silver.dbo.orders ON products.store_id = orders.store_id
JOIN 
    silver.dbo.customers ON orders.store_id = customers.store_id;


SELECT * FROM gold.dbo.retail;

use bronze

select
    stores.store_name,
    stores.ph_no,
    staffs.staff_id,
    categories.category_name,
    products.product_name,
	products.price,
    orders.order_id,
	orders.prod_id,
	orders.orders,
    customers.customer_id,
    customers.statuss,
	customers.updated_date

FROM 
    silver.dbo.stores
JOIN 
    silver.dbo.staffs ON stores.store_id = staffs.store_id
JOIN 
    silver.dbo.products ON stores.store_id = products.store_id
JOIN 
    silver.dbo.categories ON products.store_id = categories.store_id
JOIN 
    silver.dbo.orders ON products.store_id = orders.store_id
JOIN 
    silver.dbo.customers ON orders.store_id = customers.store_id;

	select store_name,ph_no,
	       staff_id,category_name,
		   product_name,order_id,
		   prod_id,price,orders,
		   customer_id,statuss,updated_date,
		   price * orders as total_price
		   into gold.dbo.retailstatus
		   from gold.dbo.retail

		   select * from gold.dbo.retailstatus
		   use gold

--------------->description<---------------------------
select 
     retailstatus.store_name, 
	 retailstatus.ph_no,   
	 retailstatus.staff_id,
	 retailstatus.category_name,
	 retailstatus.product_name,
	 retailstatus.order_id,
	 retailstatus.prod_id,
	 retailstatus.price,
	 retailstatus.orders,
	 retailstatus.customer_id,
	 retailstatus.statuss,
	 retailstatus.updated_date,
     retailstatus.total_price,
case 
    when retailstatus.store_name='fashion world' then 'mensclothing'
	when retailstatus.store_name='instore' then 'womensclothing'
	when retailstatus.store_name='diadem' then 'kidsclothing'
	when retailstatus.store_name='trends' then 'sports wear'
	when retailstatus.store_name='maharaj' then 'winterwear'
    else null
end as description
from gold.dbo.retailstatus




alter table gold.dbo.retailstatus add description varchar(30);	

insert into gold.dbo.retailstatus values('instore',4848878656,106,'womensclothing','kurtis',11111,2016,2000,7,1007,'delivered',getdate(),4500,null)

select 
     retailstatus.store_name,
case 
    when retailstatus.store_name='fashion world' then 'mensclothing'
	when retailstatus.store_name='instore' then 'womensclothing'
	when retailstatus.store_name='diadem' then 'kidsclothing'
	when retailstatus.store_name='trends' then 'sports wear'
	when retailstatus.store_name='maharaj' then 'winterwear'
    else null
end as description
from retailstatus

----------------->uppercase using case<----------------------------
select upper(store_name) from retailstatus;

select upper(store_name) from retailstatus where store_name='trends';

select 
	 retailstatus.staff_id,
	 retailstatus.ph_no,   
	 case 
     when retailstatus.store_name='trends' then upper(store_name) 
	 else store_name
end as store_Name,
	 retailstatus.category_name,
	 retailstatus.product_name,
	 retailstatus.order_id,
	 retailstatus.prod_id,
	 retailstatus.price,
	 retailstatus.orders,
	 retailstatus.customer_id,
	 retailstatus.statuss,
	 retailstatus.updated_date,
     retailstatus.total_price
from retailstatus


select 
     *,
case 
     when retail.store_name='trends' then upper(store_name)
	 else store_name
end
from gold.dbo.retail








