
delimiter $$

create or replace procedure proc_view_zip_code(metric_name varchar(32), zip_code varchar(32))
begin
    create or replace temporary table selected_metric (
        period_end datetime,
        property_type varchar(32),
        metric_value decimal(32, 24)
    );

    set @sql = concat(
        ' insert into selected_metric',
        ' select period_end, property_type, ', metric_name,
        ' from zip_code_market_tracker'
        ' where region = ''Zip Code: ', zip_code, ''';');
    prepare stmt from @sql;
    execute stmt;
    deallocate prepare stmt;

    select
        PeriodEnd,
        max(case when AllTypes is null then 0 else AllTypes end) as AllTypes,
        max(case when Condo is null then 0 else Condo end) as Condo,
        max(case when MultiFamily is null then 0 else MultiFamily end) as MultiFamily,
        max(case when SingleFamily is null then 0 else SingleFamily end) as SingleFamily,
        max(case when Townhouse is null then 0 else Townhouse end) as Townhouse
    from (
        select
            period_end as PeriodEnd,
            case when property_type = 'All Residential' then metric_value else null end as AllTypes,
            case when property_type = 'Condo/Co-op' then metric_value else null end as Condo,
            case when property_type = 'Multi-Family (2-4 Unit)' then metric_value else null end as MultiFamily,
            case when property_type = 'Single Family Residential' then metric_value else null end as SingleFamily,
            case when property_type = 'Townhouse' then metric_value else null end as Townhouse
        from selected_metric tmp
    ) tmp
    group by PeriodEnd
    order by PeriodEnd asc;
end;

$$
delimiter ;
