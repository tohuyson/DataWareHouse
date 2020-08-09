package com.datawarehouse.model;

public class Config {
	private int id;
	private String host_name;
	private int port;
	private String user_name;
	private String password;
	private String remote_path;
	private String local_path;
	private String name_file_type;
	private String name_table_staging;
	private int column_table_staging;
	private String field;
	private String field_insert;
	private String sql_create_table;
	private String name_table_warehouse;
	private String sql_insert_table;
	private String field_convert;
	
	public Config() {
		
	}

	public Config(int id, String host_name, int port, String user_name, String password, String remote_path,
			String local_path, String name_file_type, String name_table_staging, int column_table_staging, String field,
			String field_insert, String sql_create_table, String name_table_warehouse, String sql_insert_table,
			String field_convert) {
		super();
		this.id = id;
		this.host_name = host_name;
		this.port = port;
		this.user_name = user_name;
		this.password = password;
		this.remote_path = remote_path;
		this.local_path = local_path;
		this.name_file_type = name_file_type;
		this.name_table_staging = name_table_staging;
		this.column_table_staging = column_table_staging;
		this.field = field;
		this.field_insert = field_insert;
		this.sql_create_table = sql_create_table;
		this.name_table_warehouse = name_table_warehouse;
		this.sql_insert_table = sql_insert_table;
		this.field_convert = field_convert;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getHost_name() {
		return host_name;
	}

	public void setHost_name(String host_name) {
		this.host_name = host_name;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser_name() {
		return user_name;
	}

	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getRemote_path() {
		return remote_path;
	}

	public void setRemote_path(String remote_path) {
		this.remote_path = remote_path;
	}

	public String getLocal_path() {
		return local_path;
	}

	public void setLocal_path(String local_path) {
		this.local_path = local_path;
	}

	public String getName_file_type() {
		return name_file_type;
	}

	public void setName_file_type(String name_file_type) {
		this.name_file_type = name_file_type;
	}

	public String getName_table_staging() {
		return name_table_staging;
	}

	public void setName_table_staging(String name_table_staging) {
		this.name_table_staging = name_table_staging;
	}

	public int getColumn_table_staging() {
		return column_table_staging;
	}

	public void setColumn_table_staging(int column_table_staging) {
		this.column_table_staging = column_table_staging;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getField_insert() {
		return field_insert;
	}

	public void setField_insert(String field_insert) {
		this.field_insert = field_insert;
	}

	public String getSql_create_table() {
		return sql_create_table;
	}

	public void setSql_create_table(String sql_create_table) {
		this.sql_create_table = sql_create_table;
	}

	public String getName_table_warehouse() {
		return name_table_warehouse;
	}

	public void setName_table_warehouse(String name_table_warehouse) {
		this.name_table_warehouse = name_table_warehouse;
	}

	public String getSql_insert_table() {
		return sql_insert_table;
	}

	public void setSql_insert_table(String sql_insert_table) {
		this.sql_insert_table = sql_insert_table;
	}

	public String getField_convert() {
		return field_convert;
	}

	public void setField_convert(String field_convert) {
		this.field_convert = field_convert;
	}

	@Override
	public String toString() {
		return "Config [id=" + id + ", host_name=" + host_name + ", port=" + port + ", user_name=" + user_name
				+ ", password=" + password + ", remote_path=" + remote_path + ", local_path=" + local_path
				+ ", name_file_type=" + name_file_type + ", name_table_staging=" + name_table_staging
				+ ", column_table_staging=" + column_table_staging + ", field=" + field + ", field_insert="
				+ field_insert + ", sql_create_table=" + sql_create_table + ", name_table_warehouse="
				+ name_table_warehouse + ", sql_insert_table=" + sql_insert_table + ", field_convert=" + field_convert
				+ "]";
	}
	
}
