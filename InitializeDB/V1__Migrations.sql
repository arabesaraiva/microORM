CREATE DATABASE MicroORM_Sample
GO

USE MicroORM_Sample

create table Client ( 
	ID int identity primary key,
	Name varchar(50) not null,
	IsActive bit not null default(1),
	LastBuyDate datetime null)