--********************************************************
-- inference by user class - using home location 
--********************************************************

-- user break down by user class 
-- journey by user class and algorithm rule 
-- main output table : "shenzhen_bus_inference"
with inf as (
select "UserId" as userid,a."TransportMode" ,
case 
 when "EndStationInferred" like '%(NextRule)' then 'NextRule' 
 when "EndStationInferred" like '%(Stage1)' then 'Stage1' 
 when "EndStationInferred" like '%(Stage2)' then 'Stage2' 
 when "EndStationInferred" like '%(Stage3)' then 'Stage3' 
end rule_name, b.user_class
--,b.userid, * 
from "shenzhen_bus_inference" a 
left join shenzhen_user_class b on a."UserId" = b.userid 
)
--where b.userid is null 
select "TransportMode", user_class, rule_name, count(*) from inf
group by "TransportMode", user_class, rule_name

--/////////////////////////////////////////////////////////////////


--********************************************************
-- validations 
--********************************************************
-- journey by user class and algorithm rule 
-- main output table : "shenzhen_bus_valid"
with val as (
select "UserId" as userid,a."TransportMode" ,"EndStationInferred" ,
case 
 when "ValidEndStationInferred" like '%(NextRule)' then 'NextRule' 
 when "ValidEndStationInferred" like '%(Stage1)' then 'Stage1' 
 when "ValidEndStationInferred" like '%(Stage2)' then 'Stage2' 
 when "ValidEndStationInferred" like '%(Stage3)' then 'Stage3' 
end rule_name, b.user_class
--,b.userid, * 
from "shenzhen_bus_valid" a 
left join shenzhen_user_class b on a."UserId" = b.userid 
)
--where b.userid is null 
select "TransportMode", user_class, rule_name, count(*) from val
group by "TransportMode", user_class, rule_name
order by "TransportMode", user_class, rule_name





----------------- MISC queries ----------------------
------------------------------------------------- 
--6,302,666
select count(*) from  shenzhen_user_class 


-- 200,071
-- output table for inference 
select * from shenzhen_bus_inference

--129,326
-- output table for validation 
select * from shenzhen_bus_valid
limit 1000

--- 4,162,374
select count(*) from  shenzhen_user_class
where user_class not in ('Adult;Elderly','Not Useful','Not Required','Adult;Student')


-- user break down by user class 
SELECT user_class, count(*) FROM public.shenzhen_users a
left join shenzhen_user_class b on a.User_Id = b.userid 
where string_agg not in ('2') 
group by user_class

-- users and home location by user journey type
SELECT * FROM public.shenzhen_users a 
inner join public.user_info_infer b on a.user_id = b.UserId 
where b.homeFoundCount > 3 
limit 10 

-- valid by user class - using home location
------------------------------------------------------------------
SELECT user_class, count(*) FROM public.shenzhen_users a
left join shenzhen_user_class b on a.User_Id = b.userid 
where string_agg not in ('2') 
group by user_class




select b.user_class,a.*
--"UserId","EndStation","ValidEndStationInferred" 
--count(*),
--sum(case when "EndStation" = replace(replace("ValidEndStationInferred",'(Stage3)',''),'(NextRule','') then 1 else 0 end) as match
--replace(replace(replace(replace("ValidEndStationInferred",'(Stage1)',''),'(Stage2)',''),'(Stage3)',''),'(NextRule)','')
--replace(replace(replace(replace('(Stage1)(Stage2)(Stage3)(NextRule)','(Stage1)',''),'(Stage2)',''),'(Stage3)',''),'(NextRule)','')
from "shenzhen_bus_valid_NO_HL" a left join shenzhen_user_class b on a."UserId" = b.userid 
where user_class not in ('Not Required','Not Useful', 'Adult;Elderly' )
limit 1000



select 'NO_HL' as mode,
count(*),
sum(case when "EndStation" = replace(replace(replace(replace("ValidEndStationInferred",'(Stage1)',''),'(Stage2)',''),'(Stage3)',''),'(NextRule)','') then 1 else 0 end) as match
from "shenzhen_bus_valid_NO_HL" a left join shenzhen_user_class b on a."UserId" = b.userid 
where user_class not in ('Not Required','Not Useful', 'Adult;Elderly' )
union all 
select  'HL' as mode,
count(*),
sum(case when "EndStation" = replace(replace(replace(replace("ValidEndStationInferred",'(Stage1)',''),'(Stage2)',''),'(Stage3)',''),'(NextRule)','') then 1 else 0 end) as match
from "shenzhen_bus_valid" a left join shenzhen_user_class b on a."UserId" = b.userid 
where user_class not in ('Not Required','Not Useful', 'Adult;Elderly' )



select count(*) from (
SELECT a.user_id, b.homeLocation,a.string_agg FROM public.shenzhen_users a 
                    inner join public.user_info_infer b on a.user_id = b.UserId 
                    where b.homeFoundCount >= 5
                    and string_agg in ('2')) a
