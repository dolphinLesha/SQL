SET SERVEROUTPUT ON

--аудит обязательно нужен, так как я использую ddl операции в динамическом скл
create or replace package work_wth_tabs AUTHID CURRENT_USER
is

--главная процедура, второй параметр налл, значит обрабатываем рекусрию первой таблицы
procedure main_pr(table1 in varchar2,
table2 in varchar2:=null);



end work_wth_tabs;
/
create or replace package body work_wth_tabs
is
------------------------------------------------------
--имя первой таблицы
f_table_name user_tables.table_name%type;
--имя второй таблицы
s_table_name user_tables.table_name%type;

--запись, несущая всю информацию об фк, которая понадобится в ходе решения задачи
type fk_info_r is record
(
--имя ограничения
con_name user_constraints.constraint_name%type,
--имя таблицы, в которой находится ограничение
tab_name user_constraints.table_name%type,
--имя родительской по отношению связи таблицы (для исходных таблиц это налл)
p_tab_name user_constraints.table_name%type,
--имена колонок через запятую, соблюдая количество колонок в изначальном ключе (ибо в триггере мне можно менять только их)
col_name varchar2(32000),
--имена колонок всех (чтобы при изменении ограничений их и указывать)
b_col_name varchar2(32000),
--то же, но для родительского ограничения
b_p_col_name varchar2(32000),
--признак того, что мы дошли до искомой таблицы
is_accepted boolean
);
--организуем массив информации об фк
type fk_info_t is table of
fk_info_r index by pls_integer;
--запись
type PK_info_r is record
(
--пк или юк ключ исходной таблицы
pk_name user_constraints.constraint_name%type,
--массив всех фк, принадлежащих этому пк или юк
fk_info fk_info_t
);
--ну и если у нас у исходной таблицы несколько пк/юк, то записываем в массив
type pks_info_t is table of
PK_info_r index by pls_integer;
--массив для первой таблицы
pks_info1 pks_info_t;
--внешний индекс
index_table1 number:=0;
--внутренний индекс
index_table1_1 number:=0;
--массив для второй таблицы
pks_info2 pks_info_t;
--внешний индекс
index_table2 number:=0;
--внутренний индекс
index_table2_1 number:=0;

--количество колонок в изначальном пк/юк
am_cols1 number;
--режим одной или двух таблиц, по умолчанию двух
rejim boolean:=true;

------------------------------------------------------
--процедура, возвращающая имена таблиц и схемы капсом
--я объявил на всякий случай все процедуры в начале, чтобы не случилось, что я обращусь к процедуре, которая объявляется после обращения к ней
procedure get_names_of_tabs_and_schemas
(
table1 in varchar2,
table2 in varchar2,
table_1  out nocopy varchar2,
table_2 out nocopy varchar2,
schema_1 out nocopy varchar2,
schema_2 out nocopy varchar2
);
--процедура для старта поиска всех фк для всех пк/юк для первой таблицы
procedure set_pks;
--процедура для поиска всех фк для конкретного ограничения (работает по рекурсии)
procedure get_fk(cons_name in varchar2);
--процедура для старта поиска всех фк для всех пк/юк для второй таблицы
procedure set_pks2;
--процедура для поиска всех фк для конкретного ограничения (работает по рекурсии)
procedure get_fk2(cons_name in varchar2);
--процедура для старта поиска всех фк для всех пк/юк в режиме одной таблицы
procedure set_pks1;
--процедура для поиска всех фк для конкретного ограничения (работает по рекурсии)
procedure get_fk1(cons_name in varchar2);
--процедура, выводящая всю информацию, которую мы собрали (массивы)
procedure check_review;
--процедура, которая удаляет ограничения и создает их заново, но они будут осуществлять проверку после коммита, а не сразу после dml
procedure alter_tables;
--функция для проверки, а есть ли вообще свзь между таблицами
function check_ref
return boolean;
--процедура, создающая все триггеры
procedure create_trigger;
--создает триггер для рекурсивной связи (вызывается непосредственно из процедуры создания триггеров)
--поэтому первые параметры относятся к индексам ограничения
procedure create_tr_on_same(index_i in number, index_e in number, what_tab in number);

----------------------------------------------------


procedure get_names_of_tabs_and_schemas
(
table1 in varchar2,
table2 in varchar2,
table_1 out nocopy varchar2,
table_2 out nocopy varchar2,
schema_1 out nocopy varchar2,
schema_2 out nocopy varchar2
)
is
isnull exception;
v_temp varchar2(10000);
begin
--так как процедура вызывается в самом начале, и независимо от того, какой режим, то здесь можно перезатереть всю информацию
--можно было бы использовать reuse, но так как я использую рекурсию, лучше просто перезатирать таким образом переменные пакета
pks_info1.delete;
index_table1 :=0;
index_table1_1 :=0;

pks_info2.delete;
index_table2 :=0;
index_table2_1 :=0;

if table1 is null then raise isnull; end if;

--дальше просто по точке смотрим, есть ли схема в параметре, если нет, то схема не задана - выбираем пользовательскую, если есть, капосм выдаем
--создал для удобства
if regexp_count(table1,'\.')=0 then
table_1 := upper(table1);
schema_1:=user;
if regexp_count(table2,'\.')=0 then
table_2 := upper(table2);
else
table_2:=upper(substr(table2, instr(table2,'.')+1));
end if;
schema_2 := user;
else
table_1:=upper(substr(table1, instr(table1,'.')+1));
schema_1 := upper(substr(table1, 1,instr(table1,'.')-1));
schema_2 := schema_1;
if regexp_count(table2,'\.')=0 then
table_2 := upper(table2);
else
table_2:=upper(substr(table2, instr(table2,'.')+1));
end if;
end if;
f_table_name := table_1;
s_table_name := table_2;

select table_name
into v_temp
from user_tables
where table_name = f_table_name;
if s_table_name is not null then 
select table_name
into v_temp
from user_tables
where table_name = s_table_name;
end if;

exception
when isnull then
dbms_output.put_line('parametr is null');
when no_data_found then
dbms_output.put_line('have no such tables');
end;

-------------------------------------------------------------------------------------------------------------------------------set_pks1
--сразу предупрежу, что процедуры set_pks и get_fk ничем друг от друга не отличаются, только тем, что одна (с цифрой 1) работает 
--только на саму первую таблицу (потом покажу в комментарии), а остальные в обе стороны, так что логику работы можно оценить по одной из них
procedure set_pks1
is
--курсор динамический для запроса
type zapr_c is ref cursor;
cur_zapr zapr_c;

--запрос динамический
zapr varchar2(32000);

--та же информация, что в начале, но убрали родительские, добавили тип и родительское ограничение
--(использую такую запись везде в дальнейшем)
--поэтому ее можно было бы вынести как переменную пакета
type zapisi_r is record 
(
con_name user_constraints.constraint_name%type,
con_type user_constraints.constraint_type%type,
con_r_name user_constraints.constraint_name%type,
tab_name user_constraints.table_name%type,
col_name varchar2(32000)

);

zapisi zapisi_r;

begin
--режим одной таблицы
rejim:=false;
--смотрим на рекорд и понимаем, что этот запрос выделяет все пк/юк для главной таблицы с колонками
zapr:=q'[select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name, 
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.constraint_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.table_name = ']' || f_table_name || q'['
and (cons.constraint_type = 'P' or cons.constraint_type = 'U')]';
open cur_zapr for zapr;
loop
--выбираем записи
fetch cur_zapr into zapisi;
exit when cur_zapr%notfound;
index_table1_1:=0;
dbms_output.put_line(index_table1 || ' номер ключа и имя ограничения: ' || zapisi.con_name);
--заполняем первый элемент массива информацией об пк/юк, его колонками (чтобы дальше ссылаться на первый элемент при создании триггера)
pks_info1(index_table1).pk_name:=zapisi.con_name;
am_cols1:=regexp_count(zapisi.col_name,',')+1;
pks_info1(index_table1).fk_info(index_table1_1).con_name:=zapisi.con_name;
pks_info1(index_table1).fk_info(index_table1_1).tab_name:=zapisi.tab_name;
pks_info1(index_table1).fk_info(index_table1_1).col_name:=zapisi.col_name;
pks_info1(index_table1).fk_info(index_table1_1).is_accepted:=false;

index_table1_1:=index_table1_1+1;
--вызываем поиск всех фк для этого пк/юк
work_wth_tabs.get_fk1(zapisi.con_name);
index_table1:=index_table1+1;
end loop;
exception
when others then 
dbms_output.put_line('ошибка в процедуре поиска pk и uk главной таблицы');
end;

--------------------------------------------------------------------------------------------------------------------------------get_fk1
procedure get_fk1(cons_name in varchar2)
is
--курсор для динамического запроса
type zapr_c is ref cursor;
cur_zapr zapr_c;

--ну собственно все фк для ограничения-пк/юк
cursor for_f_info is
select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name, 
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.r_constraint_name, cons.table_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.r_constraint_name = cons_name; 


type zapisi_r is record 
(
con_name user_constraints.constraint_name%type,
con_type user_constraints.constraint_type%type,
con_r_name user_constraints.constraint_name%type,
tab_name user_constraints.table_name%type,
col_name varchar2(32000)

);

--мне нужно две переменные такого типа, для фк, а потом для юк (информация по юк ниже)
zapisi zapisi_r;
zapisi2 zapisi_r;
--запрос
zapr varchar2(32000);
--временные переменные
v_temp varchar2(32000);
n_temp number;
begin
open for_f_info;

loop
fetch for_f_info into zapisi;
exit when for_f_info%notfound;
--заполняем информацию по найденному фк
pks_info1(index_table1).fk_info(index_table1_1).con_name := zapisi.con_name;
pks_info1(index_table1).fk_info(index_table1_1).tab_name := zapisi.tab_name;
pks_info1(index_table1).fk_info(index_table1_1).col_name := substr(zapisi.col_name,1,instr(zapisi.col_name,',',1,am_cols1)-1);
if pks_info1(index_table1).fk_info(index_table1_1).col_name is null then
pks_info1(index_table1).fk_info(index_table1_1).col_name := zapisi.col_name;
end if;DBMS_OUTPUT.PUT_LINE('обрабатывается таблица ' || zapisi.tab_name);
--ВОТ ЗДЕСЬ ГЛАВНОЕ ОТЛИЧИЕ - так как эта процедура для режима одной таблицы, то искомая - та же таблица
if zapisi.tab_name = f_table_name then
DBMS_OUTPUT.PUT_LINE('эта таблица подходит, так как искомая ' || f_table_name);
pks_info1(index_table1).fk_info(index_table1_1).is_accepted := true;
else
DBMS_OUTPUT.PUT_LINE('эта таблица не подходит, так как искомая ' || f_table_name);
pks_info1(index_table1).fk_info(index_table1_1).is_accepted := false;
end if;
select table_name
into zapr
from user_constraints
where constraint_name = cons_name;

pks_info1(index_table1).fk_info(index_table1_1).p_tab_name:=zapr;
pks_info1(index_table1).fk_info(index_table1_1).b_col_name:=zapisi.col_name;
select distinct listagg(column_name,',') 
within group (order by position)
over (partition by constraint_name,table_name)
into zapr
from user_cons_columns
where constraint_name = cons_name;
pks_info1(index_table1).fk_info(index_table1_1).b_p_col_name :=zapr;
index_table1_1:=index_table1_1+1;
--заполнили
--обнуляем для текущего фк
n_temp:=0;
dbms_output.put_line('FK_name: ' || zapisi.con_name || '
parent constraint name: ' || zapisi.con_r_name || '
столбцы: ' || zapisi.col_name);
--теперь мы должны посмотреть все фк в этой таблице, где находится фк
--чтобы просмотреть, лежит ли фк в юк, если да, значит цепочка продолжится, если нет, то тут и заканчиваем для данного фк
zapr:=q'[select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name,
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.constraint_name, cons.table_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.table_name = ']' || zapisi.tab_name || q'['
and cons.constraint_type = 'U']';
open cur_zapr for zapr;
dbms_output.put_line('проверяем юник кеи...');
loop
fetch cur_zapr into zapisi2;
exit when cur_zapr%notfound;
n_temp:=0;
dbms_output.put_line('Unik_key name: ' || zapisi2.con_name || '
столбцы: ' ||  zapisi2.col_name);
dbms_output.put_line('проверяем, есть ли в нем наш fk');
--так как я записывал все колонки по запятым, то по ним же и разделяю, чтоб проверить, лежит ли фк в юк
--делаю таким образом, потому что порядок может быть разным, значит надо по отдельности каждую проверять
--ну то есть я беру поочередно колонку и смотрю, есть ли она в юк, если да, то увеличиваю счетчик
--если счетчик потом будет равен количеству столбиков в фк, значит лежит
for i in 0..regexp_count(zapisi2.col_name,',') loop

if i=0 then 
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, 1, instr(zapisi2.col_name,',',1,1)-1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
elsif i != 0 and i!=regexp_count(zapisi2.col_name,',') then
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, instr(zapisi2.col_name,',',1,i)+1, instr(zapisi2.col_name,',',1,i+1)-instr(zapisi2.col_name,',',1,i)-1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
else
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, instr(zapisi2.col_name,',',1,i)+1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
end if;
end loop;
--и если лежит, то вызываю эту же процедуру для юк, ну и так далее
if n_temp = regexp_count(zapisi.col_name,',')+1 then
DBMS_OUTPUT.PUT_LINE('есть, так что ищем все fk для него, если они есть');
work_wth_tabs.get_fk1(zapisi2.con_name);
else
DBMS_OUTPUT.PUT_LINE('нет');
end if;

end loop;


end loop;

exception 
when others then
dbms_output.put_line('ошибка в процедуре поиска fk');
end;

--------------------------------------------------------------------------------------------------------------------------------------set_pks
procedure set_pks
is
type zapr_c is ref cursor;
cur_zapr zapr_c;


zapr varchar2(32000);

type zapisi_r is record 
(
con_name user_constraints.constraint_name%type,
con_type user_constraints.constraint_type%type,
con_r_name user_constraints.constraint_name%type,
tab_name user_constraints.table_name%type,
col_name varchar2(32000)

);

zapisi zapisi_r;

begin
rejim:=true;
zapr:=q'[select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name, 
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.constraint_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.table_name = ']' || f_table_name || q'['
and (cons.constraint_type = 'P' or cons.constraint_type = 'U')]';
open cur_zapr for zapr;
loop
fetch cur_zapr into zapisi;
exit when cur_zapr%notfound;
index_table1_1:=0;
dbms_output.put_line(index_table1 || ' номер ключа и имя ограничения: ' || zapisi.con_name);
pks_info1(index_table1).pk_name:=zapisi.con_name;
am_cols1:=regexp_count(zapisi.col_name,',')+1;
pks_info1(index_table1).fk_info(index_table1_1).con_name:=zapisi.con_name;
pks_info1(index_table1).fk_info(index_table1_1).tab_name:=zapisi.tab_name;
pks_info1(index_table1).fk_info(index_table1_1).col_name:=zapisi.col_name;
pks_info1(index_table1).fk_info(index_table1_1).is_accepted:=false;
index_table1_1:=index_table1_1+1;
work_wth_tabs.get_fk(zapisi.con_name);
index_table1:=index_table1+1;
end loop;

exception 
when others then
dbms_output.put_line('ошибка в процедуре поиска pk и uk главной таблицы');
end;

--------------------------------------------------------------------------------------------------------------------------------------get_fk
procedure get_fk(cons_name in varchar2)
is
type zapr_c is ref cursor;
cur_zapr zapr_c;
f_table varchar2(1000) :='JOBS';
s_table varchar2(1000):='JOB_REF2';

cursor for_f_info is
select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name, 
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.r_constraint_name, cons.table_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.r_constraint_name = cons_name; 

type zapisi_r is record 
(
con_name user_constraints.constraint_name%type,
con_type user_constraints.constraint_type%type,
con_r_name user_constraints.constraint_name%type,
tab_name user_constraints.table_name%type,
col_name varchar2(32000)

);

zapisi zapisi_r;
zapisi2 zapisi_r;

zapr varchar2(32000);
v_temp varchar2(32000);
n_temp number;
begin
open for_f_info;

loop
fetch for_f_info into zapisi;
exit when for_f_info%notfound;
pks_info1(index_table1).fk_info(index_table1_1).con_name := zapisi.con_name;
pks_info1(index_table1).fk_info(index_table1_1).tab_name := zapisi.tab_name;
pks_info1(index_table1).fk_info(index_table1_1).col_name := substr(zapisi.col_name,1,instr(zapisi.col_name,',',1,am_cols1)-1);
if pks_info1(index_table1).fk_info(index_table1_1).col_name is null then
pks_info1(index_table1).fk_info(index_table1_1).col_name := zapisi.col_name;
end if;
DBMS_OUTPUT.PUT_LINE('обрабатывается таблица ' || zapisi.tab_name);

if zapisi.tab_name = s_table_name then
DBMS_OUTPUT.PUT_LINE('эта таблица подходит, так как искомая ' || s_table_name);
pks_info1(index_table1).fk_info(index_table1_1).is_accepted := true;
else
DBMS_OUTPUT.PUT_LINE('эта таблица не подходит, так как искомая ' || s_table_name);
pks_info1(index_table1).fk_info(index_table1_1).is_accepted := false;
end if;
select table_name
into zapr
from user_constraints
where constraint_name = cons_name;

pks_info1(index_table1).fk_info(index_table1_1).p_tab_name:=zapr;
pks_info1(index_table1).fk_info(index_table1_1).b_col_name:=zapisi.col_name;
select distinct listagg(column_name,',') 
within group (order by position)
over (partition by constraint_name,table_name)
into zapr
from user_cons_columns
where constraint_name = cons_name;
pks_info1(index_table1).fk_info(index_table1_1).b_p_col_name :=zapr;
index_table1_1:=index_table1_1+1;
n_temp:=0;
dbms_output.put_line('FK_name: ' || zapisi.con_name || '
parent constraint name: ' || zapisi.con_r_name || '
столбцы: ' || zapisi.col_name);
zapr:=q'[select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name,
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.constraint_name, cons.table_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.table_name = ']' || zapisi.tab_name || q'['
and cons.constraint_type = 'U']';
open cur_zapr for zapr;
dbms_output.put_line('проверяем юник кеи...');
loop
fetch cur_zapr into zapisi2;
exit when cur_zapr%notfound;
n_temp:=0;
dbms_output.put_line('Unik_key name: ' || zapisi2.con_name || '
столбцы: ' ||  zapisi2.col_name);
dbms_output.put_line('проверяем, есть ли в нем наш fk');
for i in 0..regexp_count(zapisi2.col_name,',') loop

if i=0 then 
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, 1, instr(zapisi2.col_name,',',1,1)-1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
elsif i != 0 and i!=regexp_count(zapisi2.col_name,',') then
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, instr(zapisi2.col_name,',',1,i)+1, instr(zapisi2.col_name,',',1,i+1)-instr(zapisi2.col_name,',',1,i)-1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
else
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, instr(zapisi2.col_name,',',1,i)+1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
end if;
end loop;
if n_temp = regexp_count(zapisi.col_name,',')+1 then
DBMS_OUTPUT.PUT_LINE('есть, так что ищем все fk для него, если они есть');
work_wth_tabs.get_fk(zapisi2.con_name);
else
DBMS_OUTPUT.PUT_LINE('нет');
end if;

end loop;


end loop;

exception 
when others then
dbms_output.put_line('ошибка в процедуре поиска fk');
end;

-------------------------------------------------------------------------------------------------------------------------------set_pks2
procedure set_pks2
is
type zapr_c is ref cursor;
cur_zapr zapr_c;


zapr varchar2(32000);

type zapisi_r is record 
(
con_name user_constraints.constraint_name%type,
con_type user_constraints.constraint_type%type,
con_r_name user_constraints.constraint_name%type,
tab_name user_constraints.table_name%type,
col_name varchar2(32000)

);

zapisi zapisi_r;

begin
rejim:=true;
zapr:=q'[select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name, 
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.constraint_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.table_name = ']' || s_table_name || q'['
and (cons.constraint_type = 'P' or cons.constraint_type = 'U')]';
open cur_zapr for zapr;
loop
fetch cur_zapr into zapisi;
exit when cur_zapr%notfound;
index_table2_1:=0;
dbms_output.put_line(index_table1 || ' номер ключа и имя ограничения: ' || zapisi.con_name);
pks_info2(index_table2).pk_name:=zapisi.con_name;
am_cols1:=regexp_count(zapisi.col_name,',')+1;
pks_info2(index_table2).fk_info(index_table2_1).con_name:=zapisi.con_name;
pks_info2(index_table2).fk_info(index_table2_1).tab_name:=zapisi.tab_name;
pks_info2(index_table2).fk_info(index_table2_1).col_name:=zapisi.col_name;
pks_info2(index_table2).fk_info(index_table2_1).is_accepted:=false;
index_table2_1:=index_table2_1+1;
work_wth_tabs.get_fk2(zapisi.con_name);
index_table2:=index_table2+1;
end loop;

exception 
when others then
dbms_output.put_line('ошибка в процедуре поиска pk и uk главной таблицы');
end;

--------------------------------------------------------------------------------------------------------------------------------get_fk2
procedure get_fk2(cons_name in varchar2)
is
type zapr_c is ref cursor;
cur_zapr zapr_c;
f_table varchar2(1000) :='JOBS';
s_table varchar2(1000):='JOB_REF2';

cursor for_f_info is
select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name, 
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.r_constraint_name, cons.table_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.r_constraint_name = cons_name; 

type zapisi_r is record 
(
con_name user_constraints.constraint_name%type,
con_type user_constraints.constraint_type%type,
con_r_name user_constraints.constraint_name%type,
tab_name user_constraints.table_name%type,
col_name varchar2(32000)

);

zapisi zapisi_r;
zapisi2 zapisi_r;

zapr varchar2(32000);
v_temp varchar2(32000);
n_temp number;
begin
open for_f_info;

loop
fetch for_f_info into zapisi;
exit when for_f_info%notfound;
pks_info2(index_table2).fk_info(index_table2_1).con_name := zapisi.con_name;
pks_info2(index_table2).fk_info(index_table2_1).tab_name := zapisi.tab_name;
pks_info2(index_table2).fk_info(index_table2_1).col_name := substr(zapisi.col_name,1,instr(zapisi.col_name,',',1,am_cols1)-1);
if pks_info2(index_table2).fk_info(index_table2_1).col_name is null then
pks_info2(index_table2).fk_info(index_table2_1).col_name := zapisi.col_name;
end if;
DBMS_OUTPUT.PUT_LINE('обрабатывается таблица ' || zapisi.tab_name);
if zapisi.tab_name = f_table_name then
DBMS_OUTPUT.PUT_LINE('эта таблица подходит, так как искомая ' || f_table_name);
pks_info2(index_table2).fk_info(index_table2_1).is_accepted := true;
else
DBMS_OUTPUT.PUT_LINE('эта таблица не подходит, так как искомая ' || f_table_name);
pks_info2(index_table2).fk_info(index_table2_1).is_accepted := false;
end if;
select table_name
into zapr
from user_constraints
where constraint_name = cons_name;
pks_info2(index_table2).fk_info(index_table2_1).p_tab_name:=zapr;
pks_info2(index_table2).fk_info(index_table2_1).b_col_name:=zapisi.col_name;
select distinct listagg(column_name,',') 
within group (order by position)
over (partition by constraint_name,table_name)
into zapr
from user_cons_columns
where constraint_name = cons_name;
pks_info2(index_table2).fk_info(index_table2_1).b_p_col_name :=zapr;
index_table2_1:=index_table2_1+1;
n_temp:=0;
dbms_output.put_line('FK_name: ' || zapisi.con_name || '
parent constraint name: ' || zapisi.con_r_name || '
столбцы: ' || zapisi.col_name);
zapr:=q'[select distinct cons.constraint_name, cons.constraint_type, cons.r_constraint_name, cons.table_name,
listagg(cols.column_name,',') 
within group (order by cols.position)
over (partition by cons.constraint_name, cons.table_name)
from user_constraints cons
join user_cons_columns cols
on cons.constraint_name = cols.constraint_name
where cons.table_name = ']' || zapisi.tab_name || q'['
and cons.constraint_type = 'U']';

open cur_zapr for zapr;
dbms_output.put_line('проверяем юник кеи...');
loop
fetch cur_zapr into zapisi2;
exit when cur_zapr%notfound;
n_temp:=0;
dbms_output.put_line('Unik_key name: ' || zapisi2.con_name || '
столбцы: ' ||  zapisi2.col_name);
dbms_output.put_line('проверяем, есть ли в нем наш fk');
for i in 0..regexp_count(zapisi2.col_name,',') loop

if i=0 then 
--DBMS_OUTPUT.PUT_LINE(zapisi.col_name || ' ' || substr(zapisi2.col_name, 1, instr(zapisi2.col_name,',',1,1)-1));
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, 1, instr(zapisi2.col_name,',',1,1)-1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
elsif i != 0 and i!=regexp_count(zapisi2.col_name,',') then
--DBMS_OUTPUT.PUT_LINE(zapisi.col_name || ' ' || substr(zapisi2.col_name, instr(zapisi2.col_name,',',1,i)+1, instr(zapisi2.col_name,',',1,i+1)-instr(zapisi2.col_name,',',1,i)-1));
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, instr(zapisi2.col_name,',',1,i)+1, instr(zapisi2.col_name,',',1,i+1)-instr(zapisi2.col_name,',',1,i)-1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
else
--DBMS_OUTPUT.PUT_LINE(zapisi.col_name || ' ' || substr(zapisi2.col_name, instr(zapisi2.col_name,',',1,i)+1));
if regexp_count(zapisi.col_name,'(^|,)' || substr(zapisi2.col_name, instr(zapisi2.col_name,',',1,i)+1) || '($|,)') !=0 then 
n_temp:=n_temp+1;
end if;
end if;

end loop;

if n_temp = regexp_count(zapisi.col_name,',')+1 then
DBMS_OUTPUT.PUT_LINE('есть, так что ищем все fk для него, если они есть');
work_wth_tabs.get_fk2(zapisi2.con_name);
else
DBMS_OUTPUT.PUT_LINE('нет');
end if;

end loop;


end loop;

exception 
when others then
dbms_output.put_line('ошибка в процедуре поиска fk');
end;



procedure check_review
is
--тут думаю, пояснять не надо, просто вывожу все, что накопили
begin
dbms_output.put_line('

выведем все, что нашли');
if pks_info1.count=0 then
dbms_output.put_line('в таблице ' || f_table_name || ' не было первичных или уникальных ключей');
else
for i in pks_info1.first .. pks_info1.last loop
DBMS_OUTPUT.PUT_LINE('
#' || i || 'PK/UK - ' || pks_info1(i).pk_name);
for e in pks_info1(i).fk_info.first .. pks_info1(i).fk_info.last loop
DBMS_OUTPUT.PUT_LINE('constraint name - ' || pks_info1(i).fk_info(e).con_name);
DBMS_OUTPUT.PUT_LINE('table name - ' || pks_info1(i).fk_info(e).tab_name);
DBMS_OUTPUT.PUT_LINE('columns names - ' || pks_info1(i).fk_info(e).col_name);
DBMS_OUTPUT.PUT_LINE('columns names без сокращений по изначальному количеству - ' || pks_info1(i).fk_info(e).b_col_name);
DBMS_OUTPUT.PUT_LINE('parent table name - ' || pks_info1(i).fk_info(e).p_tab_name);
DBMS_OUTPUT.PUT_LINE('parent columns names - ' || pks_info1(i).fk_info(e).b_p_col_name);
if pks_info1(i).fk_info(e).is_accepted = true then 
DBMS_OUTPUT.PUT_LINE('...ПРИНЯТ...');
else
DBMS_OUTPUT.PUT_LINE('...НЕ ПРИНЯТ...');
end if;
end loop;
end loop;
end if;

if rejim = false then 
DBMS_OUTPUT.PUT_LINE('
второй таблицы не было
');
--выходим, если режим одной таблицы, ибо второй массив пустой
return;
end if;
if pks_info2.count=0 then
dbms_output.put_line('
в таблице ' || s_table_name || ' не было первичных или уникальных ключей');
else
for i in pks_info2.first .. pks_info2.last loop
DBMS_OUTPUT.PUT_LINE('
#' || i || 'PK/UK - ' || pks_info2(i).pk_name);
for e in pks_info2(i).fk_info.first .. pks_info2(i).fk_info.last loop
DBMS_OUTPUT.PUT_LINE('constraint name - ' || pks_info2(i).fk_info(e).con_name);
DBMS_OUTPUT.PUT_LINE('table name - ' || pks_info2(i).fk_info(e).tab_name);
DBMS_OUTPUT.PUT_LINE('columns names - ' || pks_info2(i).fk_info(e).col_name);
DBMS_OUTPUT.PUT_LINE('columns names без сокращений по изначальному количеству - ' || pks_info1(i).fk_info(e).b_col_name);
DBMS_OUTPUT.PUT_LINE('parent table name - ' || pks_info2(i).fk_info(e).p_tab_name);
DBMS_OUTPUT.PUT_LINE('parent columns names - ' || pks_info2(i).fk_info(e).b_p_col_name);
if pks_info2(i).fk_info(e).is_accepted = true then 
DBMS_OUTPUT.PUT_LINE('...ПРИНЯТ...');
else
DBMS_OUTPUT.PUT_LINE('...НЕ ПРИНЯТ...');
end if;
end loop;
end loop;
end if;


exception 
when others then
dbms_output.put_line(' ошибка в процедуре вывода');
end;

--так как я не могу использовать коммиты и операции, которые вызывают автоматический коммит, такие как альтер тейбл, к примеру, в триггере
--то мне нужно стереть все ограниченияя, а потом создать заново, но чтобы они проверяли значения после коммита, вставка не вызывает его автоматически
--так что у меня получится совершить ее
--грубо говоря, я их отключаю
procedure alter_tables
is
flag1 boolean:=false;
flag2 boolean:=false;
begin
dbms_output.put_line('форматируем ограничения');
if pks_info1.count=0 then
dbms_output.put_line('в таблице ' || f_table_name || ' не было первичных или уникальных ключей');
else
for i in pks_info1.first .. pks_info1.last loop
--но обязательно сперва проверяем для каждого пк/юк исходной таблицы, связана ли по нему вторая таблица
--и если да,то для всех фк по этому пк/юк мы пересоздаем ограничения
flag1:=false;
for e in pks_info1(i).fk_info.first+1 .. pks_info1(i).fk_info.last loop
if pks_info1(i).fk_info(e).is_accepted=true then
flag1:=true;
DBMS_OUTPUT.PUT_LINE('нам подходит pk|uk ' || pks_info1(i).pk_name);
exit;
end if;

end loop;
--вот флаг тру, значит от этого пк/юк мы рано или поздно попадаем ко второй таблице, значит все нужнго обработать
if flag1=true then
DBMS_OUTPUT.PUT_LINE('
начинаем...

');
for e in pks_info1(i).fk_info.first+1 .. pks_info1(i).fk_info.last loop
DBMS_OUTPUT.PUT_LINE('alter table ' || pks_info1(i).fk_info(e).tab_name || ' drop constraint ' || pks_info1(i).fk_info(e).con_name);
DBMS_OUTPUT.PUT_LINE('alter table ' || pks_info1(i).fk_info(e).tab_name || ' add constraint ' || pks_info1(i).fk_info(e).con_name || 
' foreign key (' || pks_info1(i).fk_info(e).b_col_name || ') references ' || pks_info1(i).fk_info(e).p_tab_name || '(' || pks_info1(i).fk_info(e).b_p_col_name || ') DEFERRABLE INITIALLY DEFERRED');
execute immediate 'alter table ' || pks_info1(i).fk_info(e).tab_name || ' drop constraint ' || pks_info1(i).fk_info(e).con_name;
execute immediate 'alter table ' || pks_info1(i).fk_info(e).tab_name || ' add constraint ' || pks_info1(i).fk_info(e).con_name || 
' foreign key (' || pks_info1(i).fk_info(e).b_col_name || ') references ' || pks_info1(i).fk_info(e).p_tab_name || '(' || pks_info1(i).fk_info(e).b_p_col_name || ') DEFERRABLE INITIALLY DEFERRED';
end loop;
end if;

end loop;
end if;
DBMS_OUTPUT.PUT_LINE('
... готово

');
--то же самое и в обраную сторону, если режим двух таблиц
if rejim = false then 
return;
end if;
dbms_output.put_line('смотрим вторую таблицу');
if pks_info2.count=0 then
dbms_output.put_line('в таблице ' || s_table_name || ' не было первичных или уникальных ключей');
else
for i in pks_info2.first .. pks_info2.last loop
flag2:=false;
for e in pks_info2(i).fk_info.first+1 .. pks_info2(i).fk_info.last loop
if pks_info2(i).fk_info(e).is_accepted=true then
flag2:=true;
DBMS_OUTPUT.PUT_LINE('нам подходит pk|uk ' || pks_info2(i).pk_name);
exit;
end if;

end loop;
if flag2=true then
DBMS_OUTPUT.PUT_LINE('
начинаем...

');
for e in pks_info2(i).fk_info.first+1 .. pks_info2(i).fk_info.last loop
DBMS_OUTPUT.PUT_LINE('alter table ' || pks_info2(i).fk_info(e).tab_name || ' drop constraint ' || pks_info2(i).fk_info(e).con_name);
DBMS_OUTPUT.PUT_LINE('alter table ' || pks_info2(i).fk_info(e).tab_name || ' add constraint ' || pks_info2(i).fk_info(e).con_name || 
' foreign key (' || pks_info2(i).fk_info(e).b_col_name || ') references ' || pks_info2(i).fk_info(e).p_tab_name || '(' || pks_info2(i).fk_info(e).b_p_col_name || ') DEFERRABLE INITIALLY DEFERRED');
execute immediate 'alter table ' || pks_info2(i).fk_info(e).tab_name || ' drop constraint ' || pks_info2(i).fk_info(e).con_name;
execute immediate 'alter table ' || pks_info2(i).fk_info(e).tab_name || ' add constraint ' || pks_info2(i).fk_info(e).con_name || 
' foreign key (' || pks_info2(i).fk_info(e).b_col_name || ') references ' || pks_info2(i).fk_info(e).p_tab_name || '(' || pks_info2(i).fk_info(e).b_p_col_name || ') DEFERRABLE INITIALLY DEFERRED';
end loop;
end if;
DBMS_OUTPUT.PUT_LINE('
... готово

');
end loop;

end if;


exception 
when others then
dbms_output.put_line(' ошибка в процедуре форматирования ограничений');
end;

function check_ref
return boolean
is

begin
dbms_output.put_line('

проверяем, есть ли зависимости между таблицами/таблицей

');
--проверка зависимостей происходит так же, для каждого ключа исходной таблицы смотрим, можем ли от него попасть ко второй таблице
--(флажок будет тру), тогда можем сразу вернуть тру
if pks_info1.count=0 then
dbms_output.put_line('в таблице ' || f_table_name || ' не было первичных или уникальных ключей');
else
for i in pks_info1.first .. pks_info1.last loop
for e in pks_info1(i).fk_info.first .. pks_info1(i).fk_info.last loop
if pks_info1(i).fk_info(e).is_accepted = true then
return true;
end if;
end loop;
end loop;
end if;
if rejim = false then 
DBMS_OUTPUT.put_line('нет связей у первой таблицы');
return false;
end if;
if pks_info2.count=0 then
dbms_output.put_line('в таблице ' || s_table_name || ' не было первичных или уникальных ключей');
else
for i in pks_info2.first .. pks_info2.last loop
for e in pks_info2(i).fk_info.first .. pks_info2(i).fk_info.last loop
if pks_info2(i).fk_info(e).is_accepted = true then
return true;
end if;
end loop;
end loop;
end if;

DBMS_OUTPUT.put_line('нет связей у второй таблицы');
DBMS_OUTPUT.put_line('по итогу нет связей, нечего строить');
--если дошли сюда, значит нет никаких связей
return false;

exception 
when others then
dbms_output.put_line('ношибка в процедуре проверки зависимостей');
end;

--мозг пакета
procedure main_pr(table1 in varchar2,
table2 in varchar2:=null)
is
table_1 varchar2(32000):=table1;
table_2 varchar2(32000):=table2;
schem_1 varchar2(32000);
schem_2 varchar2(32000);
begin
dbms_output.put_line('начало работы');
--сперва капслочим все имена и выделяем схемы (с ними я не работаю)
get_names_of_tabs_and_schemas(table_1,table_2,table_1,table_2,schem_1,schem_2);
dbms_output.put_line('таблица 1: ' || table_1 ||'
таблица 2: ' || table_2);
if table_2 is null then
dbms_output.put_line('второй параметр null, так что работаем только с первой таблицей');
work_wth_tabs.set_pks1;
else
--если не налл, то вызываем в обе стороны заполнения
work_wth_tabs.set_pks;
work_wth_tabs.set_pks2;

end if;
--выводим информацию
work_wth_tabs.check_review;
--если есть связи, значит работаем дальше
if check_ref then
--пересоздаем ограничения
alter_tables;
--создаем триггеры
create_trigger;
end if;

exception 
when others then
dbms_output.put_line('ошибка в процедуре main_pr
возможные ошибки:
-не найдены
------------------------');
end;

procedure create_trigger
is
--заголовок
zagolovok varchar2(32000);
--тело (не хотелось называть body или v_body, так что пусть будет bady
bady varchar2(32000);
--признак того, что нужно вообще создавать триггер тут (если не нужно, значит ограничение рекурсивное и оно создается в следующей процедуре
--поэтому тело может быть пустым, и его создавать нет нужды абсолютно
flag boolean:=false;
--флаг того, что для этого пк/юк нужно создавать триггеры (по нему может не быть связи)
flag1 boolean :=false;
begin
if pks_info1.count=0 then
dbms_output.put_line('в таблице ' || f_table_name || ' не было первичных или уникальных ключей');
else
dbms_output.put_line('начинаем создавать триггеры');
for i in pks_info1.first .. pks_info1.last loop
flag:=false;
zagolovok:='';
bady:='';

--создаю я составной триггер, для апдейта, указываю все связанные столбцы
zagolovok:= 'create or replace trigger ' || rpad(pks_info1(i).fk_info(0).tab_name,14,'_') || '_' || rpad(pks_info1(i).fk_info(0).con_name,14,' ') || '
 for update of ' || pks_info1(i).fk_info(0).col_name || ' on ' || pks_info1(i).fk_info(0).tab_name || '
compound trigger';
--использую только один раздел перед каждой строкой (лучше тогда уж создавать одиночный триггер, ноо... создаю такой
bady:= bady || 'before each row is
begin 
';
if pks_info1(i).fk_info.count>1 then 

flag1:=false;
--если нет связи по ключу, не создаем
for e in pks_info1(i).fk_info.first+1 .. pks_info1(i).fk_info.last loop
if pks_info1(i).fk_info(e).is_accepted=true then
flag1:=true;
exit;
end if;

end loop;

if flag1=true then
dbms_output.put_line('создаем триггеры для всех связей ключа ' || pks_info1(i).pk_name);
for e in pks_info1(i).fk_info.first+1 .. pks_info1(i).fk_info.last loop
if pks_info1(i).fk_info(e).tab_name = f_table_name then
dbms_output.put_line('связь рекурсивная ' || pks_info1(i).fk_info(e).tab_name);
--и вот если таблица, в котором находится фк является той же, значит создаю для этого фк составной триггер для разрешения ситуации мутирующей таблицы
create_tr_on_same(i,e,1);
continue;
end if;
flag:=true;
bady:= bady ||
'
update ' || pks_info1(i).fk_info(e).tab_name ||'
set ';
--я обновляю только те столбцы, которые относятся к исходному ключу, заменяю все этим значением
--столбцов может быть много, поэтому разделяю так же по запятым
--остальные столбцы не трогаю, они сами заменятся, если заменятся, они для работы никакого отношения не несут, тут главное обновить нужные
for t in 0..regexp_count(pks_info1(i).fk_info(e).col_name,',') loop

if t=0 and t!= regexp_count(pks_info1(i).fk_info(e).col_name,',') then 
bady:=bady || 
substr(pks_info1(i).fk_info(e).col_name, 1, instr(pks_info1(i).fk_info(e).col_name,',',1,1)-1) || ' = :new.' ||
substr(pks_info1(i).fk_info(0).col_name, 1, instr(pks_info1(i).fk_info(0).col_name,',',1,1)-1);
elsif t != 0 and t!=regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || ',
' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1, instr(pks_info1(i).fk_info(e).col_name,',',1,t+1)-instr(pks_info1(i).fk_info(e).col_name,',',1,t)-1) || ' = :new.' ||
substr(pks_info1(i).fk_info(0).col_name, instr(pks_info1(i).fk_info(0).col_name,',',1,t)+1, instr(pks_info1(i).fk_info(0).col_name,',',1,t+1)-instr(pks_info1(i).fk_info(0).col_name,',',1,t)-1);
elsif t!=0 and t= regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || ',
' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1) || ' = :new.' ||
substr(pks_info1(i).fk_info(0).col_name, instr(pks_info1(i).fk_info(0).col_name,',',1,t)+1);
else
bady:=bady || pks_info1(i).fk_info(e).col_name || ' = :new.' || pks_info1(i).fk_info(0).col_name;
end if;
end loop;

bady:= bady || '
where ';
--раздел where
for t in 0..regexp_count(pks_info1(i).fk_info(e).col_name,',') loop

if t=0 and t!= regexp_count(pks_info1(i).fk_info(e).col_name,',') then 
bady:=bady || 
substr(pks_info1(i).fk_info(e).col_name, 1, instr(pks_info1(i).fk_info(e).col_name,',',1,1)-1) || ' = :old.' ||
substr(pks_info1(i).fk_info(0).col_name, 1, instr(pks_info1(i).fk_info(0).col_name,',',1,1)-1);
elsif t != 0 and t!=regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || '
and ' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1, instr(pks_info1(i).fk_info(e).col_name,',',1,t+1)-instr(pks_info1(i).fk_info(e).col_name,',',1,t)-1) || ' = :old.' ||
substr(pks_info1(i).fk_info(0).col_name, instr(pks_info1(i).fk_info(0).col_name,',',1,t)+1, instr(pks_info1(i).fk_info(0).col_name,',',1,t+1)-instr(pks_info1(i).fk_info(0).col_name,',',1,t)-1);
elsif t!=0 and t= regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || '
and ' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1) || ' = :old.' ||
substr(pks_info1(i).fk_info(0).col_name, instr(pks_info1(i).fk_info(0).col_name,',',1,t)+1);
else
bady:=bady || pks_info1(i).fk_info(e).col_name || ' = :old.' || pks_info1(i).fk_info(0).col_name;
end if;
end loop;
bady:= bady || '
;';
end loop;
bady:= bady || '
end before each row;';


end if;

end if;

bady:= bady || '
end;';

if flag = true then
dbms_output.put_line('

готово, результирующий триггер 

');
dbms_output.put_line(zagolovok);
dbms_output.put_line(bady);
execute immediate zagolovok || ' ' || bady;

end if;

end loop;


end if;

exception 
when others then
dbms_output.put_line('ошибка в процедуре создания триггера');
end;


procedure create_tr_on_same(index_i in number, index_e in number, what_tab in number)
is
i number:=index_i;
e number:=index_e;
zagolovok varchar2(32000);
bady varchar2(32000);

ind number;
begin


if pks_info1.count=0 then
dbms_output.put_line('в таблице ' || f_table_name || ' не было первичных или уникальных ключей');
else



zagolovok:='';
bady:='';
--принцип такой, что делаю то я все то же самое, но я загоняю старое и новое значение в переменные триггера, чтобы потом после 
--состояния апдейта перезаписать старые фк на новые пк
--поэтому тут будет два состояния, перед каждой строкой и после состояния
--создаю записи, так как столбцов может быть много
--тип переменных можно создать по пк или фк, они по идее должны иметь тот же тип данных 
zagolovok:= 'create or replace trigger ' || rpad(pks_info1(i).fk_info(e).tab_name,14,'_') || '_' || rpad(pks_info1(i).fk_info(e).con_name,14,' ') || '
 for update of ' || pks_info1(i).fk_info(e).b_p_col_name || ' on ' || pks_info1(i).fk_info(e).tab_name || '
compound trigger 
';

bady := bady || 'type rec_o is record
(';

--создаю запись для старых значений
for t in 0..regexp_count(pks_info1(i).fk_info(e).col_name,',') loop

if t=0 and t!= regexp_count(pks_info1(i).fk_info(e).col_name,',') then 
bady:=bady || 'z' || to_char(t+1) ||' ' || pks_info1(i).fk_info(e).tab_name || '.' ||
substr(pks_info1(i).fk_info(e).col_name, 1, instr(pks_info1(i).fk_info(e).col_name,',',1,1)-1) || '%type';
elsif t != 0 and t!=regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || ',
z' || to_char(t+1) ||' ' || pks_info1(i).fk_info(e).tab_name || '.' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1, instr(pks_info1(i).fk_info(e).col_name,',',1,t+1)-instr(pks_info1(i).fk_info(e).col_name,',',1,t)-1) || '%type';

elsif t!=0 and t= regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || ',
z' || to_char(t+1) ||' ' || pks_info1(i).fk_info(e).tab_name || '.' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1) || '%type';
else
bady:=bady || 'z' || to_char(t+1) ||' ' || pks_info1(i).fk_info(e).tab_name || '.' ||
pks_info1(i).fk_info(e).col_name || '%type';
end if;

end loop;
bady := bady || ');
type rec_n is record
(';
--для новых
for t in 0..regexp_count(pks_info1(i).fk_info(e).col_name,',') loop

if t=0 and t!= regexp_count(pks_info1(i).fk_info(e).col_name,',') then 
bady:=bady || 'z' || to_char(t+1) ||' ' || pks_info1(i).fk_info(e).tab_name || '.' ||
substr(pks_info1(i).fk_info(e).col_name, 1, instr(pks_info1(i).fk_info(e).col_name,',',1,1)-1) || '%type';
elsif t != 0 and t!=regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || ',
z' || to_char(t+1) ||' ' || pks_info1(i).fk_info(e).tab_name || '.' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1, instr(pks_info1(i).fk_info(e).col_name,',',1,t+1)-instr(pks_info1(i).fk_info(e).col_name,',',1,t)-1) || '%type';

elsif t!=0 and t= regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || ',
z' ||to_char(t+1)||' ' || pks_info1(i).fk_info(e).tab_name || '.' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1) || '%type';
else
bady:=bady || 'z' ||to_char(t+1) ||' ' || pks_info1(i).fk_info(e).tab_name || '.' ||
pks_info1(i).fk_info(e).col_name || '%type';
end if;
end loop;
bady := bady || ');

o_val rec_o;
n_val rec_n;';


bady:= bady || '
before each row is
begin 
';

for t in 0..regexp_count(pks_info1(i).fk_info(e).col_name,',') loop

if t=0 and t!= regexp_count(pks_info1(i).fk_info(e).col_name,',') then 
bady:=bady || 'o_val.z' || to_char(t+1) || ' := :old.' ||
substr(pks_info1(i).fk_info(e).b_p_col_name, 1, instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,1)-1) || ';';
bady:=bady || '
n_val.z' || to_char(t+1) || ' := :new.' ||
substr(pks_info1(i).fk_info(e).b_p_col_name, 1, instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,1)-1) || ';';
elsif t != 0 and t!=regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || '
o_val.z' || to_char(t+1) || ' := :old.' ||
substr(pks_info1(i).fk_info(e).b_p_col_name, instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,t)+1, instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,t+1)-instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,t)-1) || ';';
bady:=bady || '
n_val.z' || to_char(t+1) || ' := :new.' ||
substr(pks_info1(i).fk_info(e).b_p_col_name, instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,t)+1, instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,t+1)-instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,t)-1) || ';';
elsif t!=0 and t= regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || '
o_val.z' || to_char(t+1) || ' := :old.' ||
substr(pks_info1(i).fk_info(e).b_p_col_name, instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,t)+1) || ';';
bady:=bady || '
n_val.z' || to_char(t+1) || ' := :new.' ||
substr(pks_info1(i).fk_info(e).b_p_col_name, instr(pks_info1(i).fk_info(e).b_p_col_name,',',1,t)+1) || ';';
else
bady:=bady || '
o_val.z' || to_char(t+1) || ' := :old.' ||
pks_info1(i).fk_info(e).b_p_col_name || ';';
bady:=bady || '
n_val.z' || to_char(t+1) || ' := :new.' ||
pks_info1(i).fk_info(e).b_p_col_name || ';';
end if;
end loop;

bady:= bady || '
end before each row;

after statement is
begin
update ' || pks_info1(i).fk_info(e).tab_name || '
set ';


for t in 0..regexp_count(pks_info1(i).fk_info(e).col_name,',') loop

if t=0 and t!= regexp_count(pks_info1(i).fk_info(e).col_name,',') then 
bady:=bady || 
substr(pks_info1(i).fk_info(e).col_name, 1, instr(pks_info1(i).fk_info(e).col_name,',',1,1)-1) || ' = n_val.z' || to_char(t+1);
elsif t != 0 and t!=regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || ',
' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1, instr(pks_info1(i).fk_info(e).col_name,',',1,t+1)-instr(pks_info1(i).fk_info(e).col_name,',',1,t)-1) || ' = n_val.z' || to_char(t+1);
elsif t!=0 and t= regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || ',
' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1) || ' = n_val.z' || to_char(t+1);
else
bady:=bady || pks_info1(i).fk_info(e).col_name || ' = n_val.z' || to_char(t+1);
end if;
end loop;

bady:= bady || '
where ';

for t in 0..regexp_count(pks_info1(i).fk_info(e).col_name,',') loop

if t=0 and t!= regexp_count(pks_info1(i).fk_info(e).col_name,',') then 
bady:=bady || 
substr(pks_info1(i).fk_info(e).col_name, 1, instr(pks_info1(i).fk_info(e).col_name,',',1,1)-1) || ' = o_val.z' || to_char(t+1);
elsif t != 0 and t!=regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || '
and ' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1, instr(pks_info1(i).fk_info(e).col_name,',',1,t+1)-instr(pks_info1(i).fk_info(e).col_name,',',1,t)-1) || ' = o_val.z' || to_char(t+1);
elsif t!=0 and t= regexp_count(pks_info1(i).fk_info(e).col_name,',') then
bady:=bady || '
and ' ||
substr(pks_info1(i).fk_info(e).col_name, instr(pks_info1(i).fk_info(e).col_name,',',1,t)+1) || ' = o_val.z' || to_char(t+1);
else
bady:=bady || pks_info1(i).fk_info(e).col_name || ' = o_val.z' || to_char(t+1);
end if;
end loop;


bady:= bady || ';
end after statement;';



bady:= bady || '
end;';
dbms_output.put_line('

готово, создаем триггер 

');
dbms_output.put_line(zagolovok);
dbms_output.put_line(bady);
execute immediate zagolovok || ' ' || bady;
end if;

exception 
when others then
dbms_output.put_line('ошибка в процедуре создания триггера для рекурсивной связи');
end;

end work_wth_tabs;


/
declare
table1  varchar2(100):='main_table_ch';
table2  varchar2(100):='ref_table1_ch';
table_1   varchar2(100);
table_2  varchar2(100);
schema_1  varchar2(100);
schema_2  varchar2(100);
begin



work_wth_tabs.main_pr(null);



end;

/

