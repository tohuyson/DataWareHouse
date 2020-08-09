package com.datawarehouse.model;

import java.util.Date;

public class Log {
	private int id;
	private int id_config;
	private String status_download;
	private String date_time_download;
	private String local_path;
	private String name_file_local;
	private String extension;
	private String status_stagging;
	private String date_time_staging;
	private int load_row_stagging;
	private String status_warehouse;
	private String date_time_warehouse;
	private int load_row_warehouse;
	private String created_at;
	private String updated_at;
	
	public Log() {
		
	}

	public Log(int id, int id_config, String status_download, String date_time_download, String local_path,
			String name_file_local, String extension, String status_stagging, String date_time_staging,
			int load_row_stagging, String status_warehouse, String date_time_warehouse, int load_row_warehouse,
			String created_at, String updated_at) {
		super();
		this.id = id;
		this.id_config = id_config;
		this.status_download = status_download;
		this.date_time_download = date_time_download;
		this.local_path = local_path;
		this.name_file_local = name_file_local;
		this.extension = extension;
		this.status_stagging = status_stagging;
		this.date_time_staging = date_time_staging;
		this.load_row_stagging = load_row_stagging;
		this.status_warehouse = status_warehouse;
		this.date_time_warehouse = date_time_warehouse;
		this.load_row_warehouse = load_row_warehouse;
		this.created_at = created_at;
		this.updated_at = updated_at;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getId_config() {
		return id_config;
	}

	public void setId_config(int id_config) {
		this.id_config = id_config;
	}

	public String getStatus_download() {
		return status_download;
	}

	public void setStatus_download(String status_download) {
		this.status_download = status_download;
	}

	public String getDate_time_download() {
		return date_time_download;
	}

	public void setDate_time_download(String date_time_download) {
		this.date_time_download = date_time_download;
	}

	public String getLocal_path() {
		return local_path;
	}

	public void setLocal_path(String local_path) {
		this.local_path = local_path;
	}

	public String getName_file_local() {
		return name_file_local;
	}

	public void setName_file_local(String name_file_local) {
		this.name_file_local = name_file_local;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	public String getStatus_stagging() {
		return status_stagging;
	}

	public void setStatus_stagging(String status_stagging) {
		this.status_stagging = status_stagging;
	}

	public String getDate_time_staging() {
		return date_time_staging;
	}

	public void setDate_time_staging(String date_time_staging) {
		this.date_time_staging = date_time_staging;
	}

	public int getLoad_row_stagging() {
		return load_row_stagging;
	}

	public void setLoad_row_stagging(int load_row_stagging) {
		this.load_row_stagging = load_row_stagging;
	}

	public String getStatus_warehouse() {
		return status_warehouse;
	}

	public void setStatus_warehouse(String status_warehouse) {
		this.status_warehouse = status_warehouse;
	}

	public String getDate_time_warehouse() {
		return date_time_warehouse;
	}

	public void setDate_time_warehouse(String date_time_warehouse) {
		this.date_time_warehouse = date_time_warehouse;
	}

	public int getLoad_row_warehouse() {
		return load_row_warehouse;
	}

	public void setLoad_row_warehouse(int load_row_warehouse) {
		this.load_row_warehouse = load_row_warehouse;
	}

	public String getCreated_at() {
		return created_at;
	}

	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}

	public String getUpdated_at() {
		return updated_at;
	}

	public void setUpdated_at(String updated_at) {
		this.updated_at = updated_at;
	}

	@Override
	public String toString() {
		return "Log [id=" + id + ", id_config=" + id_config + ", status_download=" + status_download
				+ ", date_time_download=" + date_time_download + ", local_path=" + local_path + ", name_file_local="
				+ name_file_local + ", extension=" + extension + ", status_stagging=" + status_stagging
				+ ", date_time_staging=" + date_time_staging + ", load_row_stagging=" + load_row_stagging
				+ ", status_warehouse=" + status_warehouse + ", date_time_warehouse=" + date_time_warehouse
				+ ", load_row_warehouse=" + load_row_warehouse + ", created_at=" + created_at + ", updated_at="
				+ updated_at + "]";
	}

	
	
	
	

		
}
