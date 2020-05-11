package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDAO;
import model.entities.Department;
import model.entities.Seller;

public class SellerDAOJDBC implements SellerDAO {
	private Connection conn;
	
	public SellerDAOJDBC(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void insert(Seller sel) {
		PreparedStatement ps = null;
		
		try {
			ps = conn.prepareStatement("INSERT INTO seller (name, email, birthDate, baseSalary, departmentId) VALUES (?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, sel.getName());
			ps.setString(2, sel.getEmail());
			ps.setDate(3, new java.sql.Date(sel.getBirthDate().getTime()));
			ps.setDouble(4, sel.getBaseSalary());
			ps.setInt(5, sel.getDepartment().getId());
			
			int rowsAffected = ps.executeUpdate();
			
			if (rowsAffected > 0) {
				ResultSet rs = ps.getGeneratedKeys();
				
				if (rs.next()) {
					int id = rs.getInt(1); // pega o id gerado
					sel.setId(id);
				}
				
				DB.closeResultSet(rs);
			} else {
				throw new DbException("Unexpected error! No rows affected!");
			}
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(ps);
		}
	}

	@Override
	public void update(Seller sel) {
		PreparedStatement ps = null;
		
		try {
			ps = conn.prepareStatement("UPDATE seller SET name = ?, email = ?, birthDate = ?, baseSalary = ?, departmentId = ? WHERE id = ?");
			ps.setString(1, sel.getName());
			ps.setString(2, sel.getEmail());
			ps.setDate(3, new java.sql.Date(sel.getBirthDate().getTime()));
			ps.setDouble(4, sel.getBaseSalary());
			ps.setInt(5, sel.getDepartment().getId());
			ps.setInt(6, sel.getId());
			
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(ps);
		}
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement ps = null;
		
		try {
			ps = conn.prepareStatement("DELETE FROM seller WHERE id = ?");
			ps.setInt(1, id);
			
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(ps);
		}
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			ps = conn.prepareStatement("SELECT seller.*, department.name as DepName FROM seller INNER JOIN department " + 
					"ON seller.departmentId = department.id WHERE seller.id = ?");
			ps.setInt(1, id);
			
			rs = ps.executeQuery();
			
			if(rs.next()) { // testa se veio algum resultado
				Department dep = instantiateDepartment(rs);
				Seller sel = instantiateSeller(rs, dep);
				
				return sel;
			}
			
			return null;			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller sel = new Seller();
		sel.setId(rs.getInt("id"));
		sel.setName(rs.getString("name"));
		sel.setEmail(rs.getString("email"));
		sel.setBaseSalary(rs.getDouble("baseSalary"));
		sel.setBirthDate(rs.getDate("birthDate"));
		sel.setDepartment(dep);
		
		return sel;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("departmentId"));
		dep.setName(rs.getString("DepName"));
		
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			ps = conn.prepareStatement("SELECT seller.*, department.name as DepName FROM seller INNER JOIN department " + 
					"ON seller.departmentId = department.id ORDER BY name");
			
			rs = ps.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			
			// controle para não repitir o departamento
			Map<Integer, Department> map = new HashMap<>();
			
			while (rs.next()) { // while pq pode vir vários valores
				Department dep = map.get(rs.getInt("departmentId"));
				
				if (dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("departmentId"), dep);
				}
				
				Seller sel = instantiateSeller(rs, dep);
				list.add(sel);
			}
			
			return list;			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			ps = conn.prepareStatement("SELECT seller.*, department.name as DepName FROM seller INNER JOIN department\r\n" + 
					"ON seller.departmentId = department.id WHERE departmentId = ? ORDER BY name");
			ps.setInt(1, department.getId());
			
			rs = ps.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			
			// controle para não repitir o departamento
			Map<Integer, Department> map = new HashMap<>();
			
			while (rs.next()) { // while pq pode vir vários valores
				Department dep = map.get(rs.getInt("departmentId"));
				
				if (dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("departmentId"), dep);
				}
				
				Seller sel = instantiateSeller(rs, dep);
				list.add(sel);
			}
			
			return list;			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}
	}
}
