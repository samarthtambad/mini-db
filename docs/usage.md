#### Here is a sample of how to use each command:

```
R := inputfromfile(sales1) // import vertical bar delimited foo, first line
// has column headers.
// Suppose they are saleid|itemid|customerid|storeid|time|qty|pricerange
// In general there can be more or fewer columns than this.

R1 := select(R, (time > 50) or (qty < 30))
// select * from R where time > 50 or qty < 30

R2 := project(R1, saleid, qty, pricerange) // select saleid, qty, pricerange
// from R1

R3 := avg(R1, qty) // select avg(qty) from R1

R4 := sumgroup(R1, time, qty) // select sum(time), qty from R1 group by qty

R5 := sumgroup(R1, qty, time, pricerange) // select sum(qty), time,
// pricerange from R1 group by time, pricerange

R6 := avggroup(R1, qty, pricerange) // select avg(qty), pricerange
// from R1 group by by pricerange

R7:= countgroup(R1, qty, pricerange)

S := inputfromfile(sales2) // suppose column headers are
// saleid|I|C|S|T|Q|P

T := join(R, S, R.customerid = S.C) // select * from R, S
// where R.customerid = S.C

T1 := join(R1, S, (R1.qty > S.Q) and (R1.saleid = S.saleid)) // select * from R1, S where

T2 := sort(T1, S_C) // sort T1 by S_C
T2prime := sort(T1, R1_time, S_C) // sort T1 by R_itemid, S_C (in that order)

T3 := movavg(T2prime, R1_qty, 3) // perform the three item moving average of T2prime
// on column R_qty. This will be as long as R_qty with the three way
// moving average of 4 8 9 7 being 4 6 7 8

T4 := movsum(T2prime, R1_qty, 5) // perform the five item moving sum of T2prime
// on column R_qty

Q1 := select(R, qty = 5) // select * from R where qty=5

Btree(R, qty) // create an index on R based on column qty
// Equality selections and joins on R should use the index.
// All indexes will be on one column (both Btree and Hash)

Q2 := select(R, qty = 5) // this should use the index
Q3 := select(R, itemid = 7) // select * from R where itemid = 7

Hash(R,itemid)

Q4 := select(R, itemid = 7) // this should use the hash index
Q5 := concat(Q4, Q2) // concatenate the two tables (must have the same schema)

// Duplicate rows may result (though not with this example).
outputtofile(Q5, Q5) // This should output the table Q5 into a file

// with the same name and with vertical bar separators
outputtofile(T, T) // This should output the table T



<---- more tests --->
A:=inputfromfile(sales1)
A:=inputfromfile(sales1_small)
A1:=select(A,7=itemid)
A2:=select(A,itemid*2=14)
A3:=select(A,14=itemid*2)

B:=inputfromfile(sales2_medium)
B:=inputfromfile(sales2_small)
B:=inputfromfile(sales2)
B1:=select(B,Q<600)

D:=join(A,B,A.saleid=B.saleid)
D1:=join(A,B,A.saleid*2=B.saleid)
D2:=join(A,B,A.saleid=B.saleid/2)



D3:=join(A,B,(A.qty=B.Q)) --> -->

E:=join(A,B,(A.saleid=B.saleid) and (A.pricerange=B.P))
E1:=join(A,B,(A.saleid=B.saleid) and (A.pricerange!=B.P))
E2:=join(A,B,(A.saleid=B.saleid) and (A.qty≥B.Q))
E3:=join(A,B,(A.saleid=B.saleid) and (A.qty≤B.Q))
E3:=join(A,B,(A.qty≤B.Q) and (A.saleid=B.saleid))

E3:=join(A,B,(2*A.qty=B.Q))
E3:=join(A,B,(A.qty^2=B.Q))
E3:=join(A,B,(2*A.saleid=B.saleid))
E3:=join(A,B,(36-A.saleid=B.saleid))
E3:=join(A,B,(A.qty*2 = B.Q))
E3:=join(A,B,(A.qty*2=B.Q))


E3:=join(A,B,(A.qty=3*B.Q))
E3:=join(A,B,(A.qty=B.Q*3))

E4:=join(A,B,(B.Q<A.qty))
E3:=join(A,B,(A.qty<B.Q))

E4:=join(A,B,(B.Q<A.qty) and (A.saleid>B.saleid))
E5:=join(A,B,(B.Q≤A.qty))



F:=input


```