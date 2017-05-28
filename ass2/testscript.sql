select database();

show tables;

show create table bookauthor;

select count(isbn), sum(bookprice), sum(stock) from book;

select (select count(*) from author), (select count(*) from bookauthor);
    
select authorid, authorname, count(bookauthornumber),
	sum(bookauthornumber - (select min(bookauthornumber) from bookauthor)) as totalnum
  from bookauthor natural join author
  group by authorid, authorname
  order by totalnum;
  
select (select count(*) from bookorder), (select count(*) from orderline);

select sum(discount), sum(totalamount) from bookorder;

select sum(numcopies), sum(numcopies * bookprice) from orderline;

select *
  from bookorder B
  where totalamount + discount != (select sum(numcopies * bookprice)
									 from orderline O
                                     where O.ordernumber = B.ordernumber);
								
select (select count(*) from shipment), (select count(*) from shipmentbook);

select ordernumber, shipmentid
  from bookorder natural join shipment
  where orderdate >= shipmentdate;
  
select sum(numcopies) from shipmentbook;

select ordernumber, isbn, ordercopies, ifnull(shipcopies, 0) as shipcopies
from (select ordernumber, isbn, numcopies as ordercopies
        from bookorder natural join orderline) as orders
     natural left join
     (select ordernumber, isbn, sum(numcopies) as shipcopies
        from shipment natural join shipmentbook
        group by ordernumber, isbn) as shipments
  where ordercopies != ifnull(shipcopies, 0)
  order by ordernumber, isbn;
