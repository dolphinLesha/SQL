SET SERVEROUTPUT ON

--����� ����������� �����, ��� ��� � ��������� ddl �������� � ������������ ���
create or replace package work_wth_tabs AUTHID CURRENT_USER
is

--������� ���������, ������ �������� ����, ������ ������������ �������� ������ �������
procedure main_pr(table1 in varchar2,
table2 in varchar2:=null);



end work_wth_tabs;
/
create or replace package body work_wth_tabs
is
------------------------------------------------------
--��� ������ �������
f_table_name user_tables.table_name%type;
--��� ������ �������
s_table_name user_tables.table_name%type;

--������, ������� ��� ���������� �� ��, ������� ����������� � ���� ������� ������
type fk_info_r is record
(
--��� �����������
con_name user_constraints.constraint_name%type,
--��� �������, � ������� ��������� �����������
tab_name user_constraints.table_name%type,
--��� ������������ �� ��������� ����� ������� (��� �������� ������ ��� ����)
p_tab_name user_constraints.table_name%type,
--����� ������� ����� �������, �������� ���������� ������� � ����������� ����� (��� � �������� ��� ����� ������ ������ ��)
col_name varchar2(32000),
--����� ������� ���� (����� ��� ��������� ����������� �� � ���������)
b_col_name varchar2(32000),
--�� ��, �� ��� ������������� �����������
b_p_col_name varchar2(32000),
--������� ����, ��� �� ����� �� ������� �������
is_accepted boolean
);
--���������� ������ ���������� �� ��
type fk_info_t is table of
fk_info_r index by pls_integer;
--������
type PK_info_r is record
(
--�� ��� �� ���� �������� �������
pk_name user_constraints.constraint_name%type,
--������ ���� ��, ������������� ����� �� ��� ��
fk_info fk_info_t
);
--�� � ���� � ��� � �������� ������� ��������� ��/��, �� ���������� � ������
type pks_info_t is table of
PK_info_r index by pls_integer;
--������ ��� ������ �������
pks_info1 pks_info_t;
--������� ������
index_table1 number:=0;
--���������� ������
index_table1_1 number:=0;
--������ ��� ������ �������
pks_info2 pks_info_t;
--������� ������
index_table2 number:=0;
--���������� ������
index_table2_1 number:=0;

--���������� ������� � ����������� ��/��
am_cols1 number;
--����� ����� ��� ���� ������, �� ��������� ����
rejim boolean:=true;

------------------------------------------------------
--���������, ������������ ����� ������ � ����� ������
--� ������� �� ������ ������ ��� ��������� � ������, ����� �� ���������, ��� � �������� � ���������, ������� ����������� ����� ��������� � ���
procedure get_names_of_tabs_and_schemas
(
table1 in varchar2,
table2 in varchar2,
table_1  out nocopy varchar2,
table_2 out nocopy varchar2,
schema_1 out nocopy varchar2,
schema_2 out nocopy varchar2
);
--��������� ��� ������ ������ ���� �� ��� ���� ��/�� ��� ������ �������
procedure set_pks;
--��������� ��� ������ ���� �� ��� ����������� ����������� (�������� �� ��������)
procedure get_fk(cons_name in varchar2);
--��������� ��� ������ ������ ���� �� ��� ���� ��/�� ��� ������ �������
procedure set_pks2;
--��������� ��� ������ ���� �� ��� ����������� ����������� (�������� �� ��������)
procedure get_fk2(cons_name in varchar2);
--��������� ��� ������ ������ ���� �� ��� ���� ��/�� � ������ ����� �������
procedure set_pks1;
--��������� ��� ������ ���� �� ��� ����������� ����������� (�������� �� ��������)
procedure get_fk1(cons_name in varchar2);
--���������, ��������� ��� ����������, ������� �� ������� (�������)
procedure check_review;
--���������, ������� ������� ����������� � ������� �� ������, �� ��� ����� ������������ �������� ����� �������, � �� ����� ����� dml
procedure alter_tables;
--������� ��� ��������, � ���� �� ������ ���� ����� ���������
function check_ref
return boolean;
--���������, ��������� ��� ��������
procedure create_trigger;
--������� ������� ��� ����������� ����� (���������� ��������������� �� ��������� �������� ���������)
--������� ������ ��������� ��������� � �������� �����������
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
--��� ��� ��������� ���������� � ����� ������, � ���������� �� ����, ����� �����, �� ����� ����� ������������ ��� ����������
--����� ���� �� ������������ reuse, �� ��� ��� � ��������� ��������, ����� ������ ������������ ����� ������� ���������� ������
pks_info1.delete;
index_table1 :=0;
index_table1_1 :=0;

pks_info2.delete;
index_table2 :=0;
index_table2_1 :=0;

if table1 is null then raise isnull; end if;

--������ ������ �� ����� �������, ���� �� ����� � ���������, ���� ���, �� ����� �� ������ - �������� ����������������, ���� ����, ������ ������
--������ ��� ��������
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
--����� ����������, ��� ��������� set_pks � get_fk ����� ���� �� ����� �� ����������, ������ ���, ��� ���� (� ������ 1) �������� 
--������ �� ���� ������ ������� (����� ������ � �����������), � ��������� � ��� �������, ��� ��� ������ ������ ����� ������� �� ����� �� ���
procedure set_pks1
is
--������ ������������ ��� �������
type zapr_c is ref cursor;
cur_zapr zapr_c;

--������ ������������
zapr varchar2(32000);

--�� �� ����������, ��� � ������, �� ������ ������������, �������� ��� � ������������ �����������
--(��������� ����� ������ ����� � ����������)
--������� �� ����� ���� �� ������� ��� ���������� ������
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
--����� ����� �������
rejim:=false;
--������� �� ������ � ��������, ��� ���� ������ �������� ��� ��/�� ��� ������� ������� � ���������
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
--�������� ������
fetch cur_zapr into zapisi;
exit when cur_zapr%notfound;
index_table1_1:=0;
dbms_output.put_line(index_table1 || ' ����� ����� � ��� �����������: ' || zapisi.con_name);
--��������� ������ ������� ������� ����������� �� ��/��, ��� ��������� (����� ������ ��������� �� ������ ������� ��� �������� ��������)
pks_info1(index_table1).pk_name:=zapisi.con_name;
am_cols1:=regexp_count(zapisi.col_name,',')+1;
pks_info1(index_table1).fk_info(index_table1_1).con_name:=zapisi.con_name;
pks_info1(index_table1).fk_info(index_table1_1).tab_name:=zapisi.tab_name;
pks_info1(index_table1).fk_info(index_table1_1).col_name:=zapisi.col_name;
pks_info1(index_table1).fk_info(index_table1_1).is_accepted:=false;

index_table1_1:=index_table1_1+1;
--�������� ����� ���� �� ��� ����� ��/��
work_wth_tabs.get_fk1(zapisi.con_name);
index_table1:=index_table1+1;
end loop;
exception
when others then 
dbms_output.put_line('������ � ��������� ������ pk � uk ������� �������');
end;

--------------------------------------------------------------------------------------------------------------------------------get_fk1
procedure get_fk1(cons_name in varchar2)
is
--������ ��� ������������� �������
type zapr_c is ref cursor;
cur_zapr zapr_c;

--�� ���������� ��� �� ��� �����������-��/��
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

--��� ����� ��� ���������� ������ ����, ��� ��, � ����� ��� �� (���������� �� �� ����)
zapisi zapisi_r;
zapisi2 zapisi_r;
--������
zapr varchar2(32000);
--��������� ����������
v_temp varchar2(32000);
n_temp number;
begin
open for_f_info;

loop
fetch for_f_info into zapisi;
exit when for_f_info%notfound;
--��������� ���������� �� ���������� ��
pks_info1(index_table1).fk_info(index_table1_1).con_name := zapisi.con_name;
pks_info1(index_table1).fk_info(index_table1_1).tab_name := zapisi.tab_name;
pks_info1(index_table1).fk_info(index_table1_1).col_name := substr(zapisi.col_name,1,instr(zapisi.col_name,',',1,am_cols1)-1);
if pks_info1(index_table1).fk_info(index_table1_1).col_name is null then
pks_info1(index_table1).fk_info(index_table1_1).col_name := zapisi.col_name;
end if;DBMS_OUTPUT.PUT_LINE('�������������� ������� ' || zapisi.tab_name);
--��� ����� ������� ������� - ��� ��� ��� ��������� ��� ������ ����� �������, �� ������� - �� �� �������
if zapisi.tab_name = f_table_name then
DBMS_OUTPUT.PUT_LINE('��� ������� ��������, ��� ��� ������� ' || f_table_name);
pks_info1(index_table1).fk_info(index_table1_1).is_accepted := true;
else
DBMS_OUTPUT.PUT_LINE('��� ������� �� ��������, ��� ��� ������� ' || f_table_name);
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
--���������
--�������� ��� �������� ��
n_temp:=0;
dbms_output.put_line('FK_name: ' || zapisi.con_name || '
parent constraint name: ' || zapisi.con_r_name || '
�������: ' || zapisi.col_name);
--������ �� ������ ���������� ��� �� � ���� �������, ��� ��������� ��
--����� �����������, ����� �� �� � ��, ���� ��, ������ ������� �����������, ���� ���, �� ��� � ����������� ��� ������� ��
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
dbms_output.put_line('��������� ���� ���...');
loop
fetch cur_zapr into zapisi2;
exit when cur_zapr%notfound;
n_temp:=0;
dbms_output.put_line('Unik_key name: ' || zapisi2.con_name || '
�������: ' ||  zapisi2.col_name);
dbms_output.put_line('���������, ���� �� � ��� ��� fk');
--��� ��� � ��������� ��� ������� �� �������, �� �� ��� �� � ��������, ���� ���������, ����� �� �� � ��
--����� ����� �������, ������ ��� ������� ����� ���� ������, ������ ���� �� ����������� ������ ���������
--�� �� ���� � ���� ���������� ������� � ������, ���� �� ��� � ��, ���� ��, �� ���������� �������
--���� ������� ����� ����� ����� ���������� ��������� � ��, ������ �����
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
--� ���� �����, �� ������� ��� �� ��������� ��� ��, �� � ��� �����
if n_temp = regexp_count(zapisi.col_name,',')+1 then
DBMS_OUTPUT.PUT_LINE('����, ��� ��� ���� ��� fk ��� ����, ���� ��� ����');
work_wth_tabs.get_fk1(zapisi2.con_name);
else
DBMS_OUTPUT.PUT_LINE('���');
end if;

end loop;


end loop;

exception 
when others then
dbms_output.put_line('������ � ��������� ������ fk');
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
dbms_output.put_line(index_table1 || ' ����� ����� � ��� �����������: ' || zapisi.con_name);
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
dbms_output.put_line('������ � ��������� ������ pk � uk ������� �������');
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
DBMS_OUTPUT.PUT_LINE('�������������� ������� ' || zapisi.tab_name);

if zapisi.tab_name = s_table_name then
DBMS_OUTPUT.PUT_LINE('��� ������� ��������, ��� ��� ������� ' || s_table_name);
pks_info1(index_table1).fk_info(index_table1_1).is_accepted := true;
else
DBMS_OUTPUT.PUT_LINE('��� ������� �� ��������, ��� ��� ������� ' || s_table_name);
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
�������: ' || zapisi.col_name);
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
dbms_output.put_line('��������� ���� ���...');
loop
fetch cur_zapr into zapisi2;
exit when cur_zapr%notfound;
n_temp:=0;
dbms_output.put_line('Unik_key name: ' || zapisi2.con_name || '
�������: ' ||  zapisi2.col_name);
dbms_output.put_line('���������, ���� �� � ��� ��� fk');
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
DBMS_OUTPUT.PUT_LINE('����, ��� ��� ���� ��� fk ��� ����, ���� ��� ����');
work_wth_tabs.get_fk(zapisi2.con_name);
else
DBMS_OUTPUT.PUT_LINE('���');
end if;

end loop;


end loop;

exception 
when others then
dbms_output.put_line('������ � ��������� ������ fk');
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
dbms_output.put_line(index_table1 || ' ����� ����� � ��� �����������: ' || zapisi.con_name);
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
dbms_output.put_line('������ � ��������� ������ pk � uk ������� �������');
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
DBMS_OUTPUT.PUT_LINE('�������������� ������� ' || zapisi.tab_name);
if zapisi.tab_name = f_table_name then
DBMS_OUTPUT.PUT_LINE('��� ������� ��������, ��� ��� ������� ' || f_table_name);
pks_info2(index_table2).fk_info(index_table2_1).is_accepted := true;
else
DBMS_OUTPUT.PUT_LINE('��� ������� �� ��������, ��� ��� ������� ' || f_table_name);
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
�������: ' || zapisi.col_name);
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
dbms_output.put_line('��������� ���� ���...');
loop
fetch cur_zapr into zapisi2;
exit when cur_zapr%notfound;
n_temp:=0;
dbms_output.put_line('Unik_key name: ' || zapisi2.con_name || '
�������: ' ||  zapisi2.col_name);
dbms_output.put_line('���������, ���� �� � ��� ��� fk');
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
DBMS_OUTPUT.PUT_LINE('����, ��� ��� ���� ��� fk ��� ����, ���� ��� ����');
work_wth_tabs.get_fk2(zapisi2.con_name);
else
DBMS_OUTPUT.PUT_LINE('���');
end if;

end loop;


end loop;

exception 
when others then
dbms_output.put_line('������ � ��������� ������ fk');
end;



procedure check_review
is
--��� �����, �������� �� ����, ������ ������ ���, ��� ��������
begin
dbms_output.put_line('

������� ���, ��� �����');
if pks_info1.count=0 then
dbms_output.put_line('� ������� ' || f_table_name || ' �� ���� ��������� ��� ���������� ������');
else
for i in pks_info1.first .. pks_info1.last loop
DBMS_OUTPUT.PUT_LINE('
#' || i || 'PK/UK - ' || pks_info1(i).pk_name);
for e in pks_info1(i).fk_info.first .. pks_info1(i).fk_info.last loop
DBMS_OUTPUT.PUT_LINE('constraint name - ' || pks_info1(i).fk_info(e).con_name);
DBMS_OUTPUT.PUT_LINE('table name - ' || pks_info1(i).fk_info(e).tab_name);
DBMS_OUTPUT.PUT_LINE('columns names - ' || pks_info1(i).fk_info(e).col_name);
DBMS_OUTPUT.PUT_LINE('columns names ��� ���������� �� ������������ ���������� - ' || pks_info1(i).fk_info(e).b_col_name);
DBMS_OUTPUT.PUT_LINE('parent table name - ' || pks_info1(i).fk_info(e).p_tab_name);
DBMS_OUTPUT.PUT_LINE('parent columns names - ' || pks_info1(i).fk_info(e).b_p_col_name);
if pks_info1(i).fk_info(e).is_accepted = true then 
DBMS_OUTPUT.PUT_LINE('...������...');
else
DBMS_OUTPUT.PUT_LINE('...�� ������...');
end if;
end loop;
end loop;
end if;

if rejim = false then 
DBMS_OUTPUT.PUT_LINE('
������ ������� �� ����
');
--�������, ���� ����� ����� �������, ��� ������ ������ ������
return;
end if;
if pks_info2.count=0 then
dbms_output.put_line('
� ������� ' || s_table_name || ' �� ���� ��������� ��� ���������� ������');
else
for i in pks_info2.first .. pks_info2.last loop
DBMS_OUTPUT.PUT_LINE('
#' || i || 'PK/UK - ' || pks_info2(i).pk_name);
for e in pks_info2(i).fk_info.first .. pks_info2(i).fk_info.last loop
DBMS_OUTPUT.PUT_LINE('constraint name - ' || pks_info2(i).fk_info(e).con_name);
DBMS_OUTPUT.PUT_LINE('table name - ' || pks_info2(i).fk_info(e).tab_name);
DBMS_OUTPUT.PUT_LINE('columns names - ' || pks_info2(i).fk_info(e).col_name);
DBMS_OUTPUT.PUT_LINE('columns names ��� ���������� �� ������������ ���������� - ' || pks_info1(i).fk_info(e).b_col_name);
DBMS_OUTPUT.PUT_LINE('parent table name - ' || pks_info2(i).fk_info(e).p_tab_name);
DBMS_OUTPUT.PUT_LINE('parent columns names - ' || pks_info2(i).fk_info(e).b_p_col_name);
if pks_info2(i).fk_info(e).is_accepted = true then 
DBMS_OUTPUT.PUT_LINE('...������...');
else
DBMS_OUTPUT.PUT_LINE('...�� ������...');
end if;
end loop;
end loop;
end if;


exception 
when others then
dbms_output.put_line(' ������ � ��������� ������');
end;

--��� ��� � �� ���� ������������ ������� � ��������, ������� �������� �������������� ������, ����� ��� ������ �����, � �������, � ��������
--�� ��� ����� ������� ��� ������������, � ����� ������� ������, �� ����� ��� ��������� �������� ����� �������, ������� �� �������� ��� �������������
--��� ��� � ���� ��������� ��������� ��
--����� ������, � �� ��������
procedure alter_tables
is
flag1 boolean:=false;
flag2 boolean:=false;
begin
dbms_output.put_line('����������� �����������');
if pks_info1.count=0 then
dbms_output.put_line('� ������� ' || f_table_name || ' �� ���� ��������� ��� ���������� ������');
else
for i in pks_info1.first .. pks_info1.last loop
--�� ����������� ������ ��������� ��� ������� ��/�� �������� �������, ������� �� �� ���� ������ �������
--� ���� ��,�� ��� ���� �� �� ����� ��/�� �� ����������� �����������
flag1:=false;
for e in pks_info1(i).fk_info.first+1 .. pks_info1(i).fk_info.last loop
if pks_info1(i).fk_info(e).is_accepted=true then
flag1:=true;
DBMS_OUTPUT.PUT_LINE('��� �������� pk|uk ' || pks_info1(i).pk_name);
exit;
end if;

end loop;
--��� ���� ���, ������ �� ����� ��/�� �� ���� ��� ������ �������� �� ������ �������, ������ ��� ������ ����������
if flag1=true then
DBMS_OUTPUT.PUT_LINE('
��������...

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
... ������

');
--�� �� ����� � � ������� �������, ���� ����� ���� ������
if rejim = false then 
return;
end if;
dbms_output.put_line('������� ������ �������');
if pks_info2.count=0 then
dbms_output.put_line('� ������� ' || s_table_name || ' �� ���� ��������� ��� ���������� ������');
else
for i in pks_info2.first .. pks_info2.last loop
flag2:=false;
for e in pks_info2(i).fk_info.first+1 .. pks_info2(i).fk_info.last loop
if pks_info2(i).fk_info(e).is_accepted=true then
flag2:=true;
DBMS_OUTPUT.PUT_LINE('��� �������� pk|uk ' || pks_info2(i).pk_name);
exit;
end if;

end loop;
if flag2=true then
DBMS_OUTPUT.PUT_LINE('
��������...

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
... ������

');
end loop;

end if;


exception 
when others then
dbms_output.put_line(' ������ � ��������� �������������� �����������');
end;

function check_ref
return boolean
is

begin
dbms_output.put_line('

���������, ���� �� ����������� ����� ���������/��������

');
--�������� ������������ ���������� ��� ��, ��� ������� ����� �������� ������� �������, ����� �� �� ���� ������� �� ������ �������
--(������ ����� ���), ����� ����� ����� ������� ���
if pks_info1.count=0 then
dbms_output.put_line('� ������� ' || f_table_name || ' �� ���� ��������� ��� ���������� ������');
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
DBMS_OUTPUT.put_line('��� ������ � ������ �������');
return false;
end if;
if pks_info2.count=0 then
dbms_output.put_line('� ������� ' || s_table_name || ' �� ���� ��������� ��� ���������� ������');
else
for i in pks_info2.first .. pks_info2.last loop
for e in pks_info2(i).fk_info.first .. pks_info2(i).fk_info.last loop
if pks_info2(i).fk_info(e).is_accepted = true then
return true;
end if;
end loop;
end loop;
end if;

DBMS_OUTPUT.put_line('��� ������ � ������ �������');
DBMS_OUTPUT.put_line('�� ����� ��� ������, ������ �������');
--���� ����� ����, ������ ��� ������� ������
return false;

exception 
when others then
dbms_output.put_line('������� � ��������� �������� ������������');
end;

--���� ������
procedure main_pr(table1 in varchar2,
table2 in varchar2:=null)
is
table_1 varchar2(32000):=table1;
table_2 varchar2(32000):=table2;
schem_1 varchar2(32000);
schem_2 varchar2(32000);
begin
dbms_output.put_line('������ ������');
--������ ��������� ��� ����� � �������� ����� (� ���� � �� �������)
get_names_of_tabs_and_schemas(table_1,table_2,table_1,table_2,schem_1,schem_2);
dbms_output.put_line('������� 1: ' || table_1 ||'
������� 2: ' || table_2);
if table_2 is null then
dbms_output.put_line('������ �������� null, ��� ��� �������� ������ � ������ ��������');
work_wth_tabs.set_pks1;
else
--���� �� ����, �� �������� � ��� ������� ����������
work_wth_tabs.set_pks;
work_wth_tabs.set_pks2;

end if;
--������� ����������
work_wth_tabs.check_review;
--���� ���� �����, ������ �������� ������
if check_ref then
--����������� �����������
alter_tables;
--������� ��������
create_trigger;
end if;

exception 
when others then
dbms_output.put_line('������ � ��������� main_pr
��������� ������:
-�� �������
------------------------');
end;

procedure create_trigger
is
--���������
zagolovok varchar2(32000);
--���� (�� �������� �������� body ��� v_body, ��� ��� ����� ����� bady
bady varchar2(32000);
--������� ����, ��� ����� ������ ��������� ������� ��� (���� �� �����, ������ ����������� ����������� � ��� ��������� � ��������� ���������
--������� ���� ����� ���� ������, � ��� ��������� ��� ����� ���������
flag boolean:=false;
--���� ����, ��� ��� ����� ��/�� ����� ��������� �������� (�� ���� ����� �� ���� �����)
flag1 boolean :=false;
begin
if pks_info1.count=0 then
dbms_output.put_line('� ������� ' || f_table_name || ' �� ���� ��������� ��� ���������� ������');
else
dbms_output.put_line('�������� ��������� ��������');
for i in pks_info1.first .. pks_info1.last loop
flag:=false;
zagolovok:='';
bady:='';

--������ � ��������� �������, ��� �������, �������� ��� ��������� �������
zagolovok:= 'create or replace trigger ' || rpad(pks_info1(i).fk_info(0).tab_name,14,'_') || '_' || rpad(pks_info1(i).fk_info(0).con_name,14,' ') || '
 for update of ' || pks_info1(i).fk_info(0).col_name || ' on ' || pks_info1(i).fk_info(0).tab_name || '
compound trigger';
--��������� ������ ���� ������ ����� ������ ������� (����� ����� �� ��������� ��������� �������, ���... ������ �����
bady:= bady || 'before each row is
begin 
';
if pks_info1(i).fk_info.count>1 then 

flag1:=false;
--���� ��� ����� �� �����, �� �������
for e in pks_info1(i).fk_info.first+1 .. pks_info1(i).fk_info.last loop
if pks_info1(i).fk_info(e).is_accepted=true then
flag1:=true;
exit;
end if;

end loop;

if flag1=true then
dbms_output.put_line('������� �������� ��� ���� ������ ����� ' || pks_info1(i).pk_name);
for e in pks_info1(i).fk_info.first+1 .. pks_info1(i).fk_info.last loop
if pks_info1(i).fk_info(e).tab_name = f_table_name then
dbms_output.put_line('����� ����������� ' || pks_info1(i).fk_info(e).tab_name);
--� ��� ���� �������, � ������� ��������� �� �������� ��� ��, ������ ������ ��� ����� �� ��������� ������� ��� ���������� �������� ���������� �������
create_tr_on_same(i,e,1);
continue;
end if;
flag:=true;
bady:= bady ||
'
update ' || pks_info1(i).fk_info(e).tab_name ||'
set ';
--� �������� ������ �� �������, ������� ��������� � ��������� �����, ������� ��� ���� ���������
--�������� ����� ���� �����, ������� �������� ��� �� �� �������
--��������� ������� �� ������, ��� ���� ���������, ���� ���������, ��� ��� ������ �������� ��������� �� �����, ��� ������� �������� ������
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
--������ where
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

������, �������������� ������� 

');
dbms_output.put_line(zagolovok);
dbms_output.put_line(bady);
execute immediate zagolovok || ' ' || bady;

end if;

end loop;


end if;

exception 
when others then
dbms_output.put_line('������ � ��������� �������� ��������');
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
dbms_output.put_line('� ������� ' || f_table_name || ' �� ���� ��������� ��� ���������� ������');
else



zagolovok:='';
bady:='';
--������� �����, ��� ����� �� � ��� �� �� �����, �� � ������� ������ � ����� �������� � ���������� ��������, ����� ����� ����� 
--��������� ������� ������������ ������ �� �� ����� ��
--������� ��� ����� ��� ���������, ����� ������ ������� � ����� ���������
--������ ������, ��� ��� �������� ����� ���� �����
--��� ���������� ����� ������� �� �� ��� ��, ��� �� ���� ������ ����� ��� �� ��� ������ 
zagolovok:= 'create or replace trigger ' || rpad(pks_info1(i).fk_info(e).tab_name,14,'_') || '_' || rpad(pks_info1(i).fk_info(e).con_name,14,' ') || '
 for update of ' || pks_info1(i).fk_info(e).b_p_col_name || ' on ' || pks_info1(i).fk_info(e).tab_name || '
compound trigger 
';

bady := bady || 'type rec_o is record
(';

--������ ������ ��� ������ ��������
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
--��� �����
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

������, ������� ������� 

');
dbms_output.put_line(zagolovok);
dbms_output.put_line(bady);
execute immediate zagolovok || ' ' || bady;
end if;

exception 
when others then
dbms_output.put_line('������ � ��������� �������� �������� ��� ����������� �����');
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

