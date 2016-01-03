create table "users" (
  "id" integer not null,
  "name" character varying(50) not null,
  "email" character varying(50) not null,
  constraint "users_pk" primary key ("id")
);

create table "roles" (
  "id" integer not null,
  "name" varchar(100) not null,
  constraint "roles_pk" primary key ("id")
);

create table "users_with_roles" (
  "user_id" integer not null,
  "role_id" integer not null,
  constraint "users_with_roles_pk" primary key ("user_id","role_id"),
  constraint "users_with_roles_user_fk" foreign key ("user_id") references "users"("id") on update cascade on delete restrict,
  constraint "users_with_roles_role_fk" foreign key ("role_id") references "roles"("id") on update cascade on delete restrict
);

insert into "users" ("id", "name", "email") values
    (1, 'Stephan', 'stephan.goertz@gmail.com'),
    (2, 'Steffi', 'steffi05.04@freenet.de'),
    (3, 'Willi', 'willi@web.de'),
    (4, 'Franz', 'franz@web.de')
;

insert into "roles" ("id", "name") values
    (1, 'Admin'),
    (2, 'Developer'),
    (3, 'Designer')
;

insert into "users_with_roles" ("user_id", "role_id") values
    (1, 1),
    (1, 2),
    (2, 3),
    (4, 2)
;