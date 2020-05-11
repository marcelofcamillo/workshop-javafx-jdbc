package model.services;

import java.util.List;

import model.dao.DAOFactory;
import model.dao.DepartmentDAO;
import model.entities.Department;

public class DepartmentService {
	private DepartmentDAO dao = DAOFactory.createDepartmentDAO();
	
	public List<Department> findAll() {
		return dao.findAll();
		
		/* MOCK
		 * List<Department> list = new ArrayList<>(); list.add(new Department(1,
		 * "Books")); list.add(new Department(2, "Computers")); list.add(new
		 * Department(3, "Electronics"));
		 * 
		 * return list; */
	}
}
