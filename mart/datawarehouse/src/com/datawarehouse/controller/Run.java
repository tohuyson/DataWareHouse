package com.datawarehouse.controller;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@WebServlet("/Run")
public class Run extends HttpServlet {
	private static final long serialVersionUID = 1L;
 
    public Run() {
       
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Runtime.
		   getRuntime().
		   exec("cmd /c start \"\" E:\\workspace2\\datawarehouse\\run.bat");
		response.sendRedirect("New");
	}

}
