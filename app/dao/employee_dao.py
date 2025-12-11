# app/dao/employee_dao.py

import pymysql
import pymysql.err
from flask import current_app 

class EmployeeDAO:
    
    def get_db_connection(self):
        config = current_app.config 
        return pymysql.connect(
            host=config['MYSQL_HOST'],
            user=config['MYSQL_USER'],
            password=config['MYSQL_PASSWORD'],
            db=config['MYSQL_DB'],
            cursorclass=pymysql.cursors.DictCursor
        )

    # ----------------------------------------
    # I. EMPLOYEE CRUD 
    # ----------------------------------------
    
    def create_employee(self, data):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO employees 
            (first_name, last_name, email, department_id, is_it_staff) 
            VALUES (%s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(sql, (
                data['first_name'], 
                data['last_name'], 
                data['email'], 
                data['department_id'],
                data.get('is_it_staff', False)
            ))
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            print(f"Error creating employee: {e}")
            conn.rollback() 
            return None
        finally:
            cursor.close()
            conn.close() 

    def get_employee_by_id(self, employee_id):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = "SELECT * FROM employees WHERE employee_id = %s"
        try:
            cursor.execute(sql, (employee_id,))
            employee = cursor.fetchone()
            return employee
        finally:
            cursor.close()
            conn.close()

    def get_all_employees(self):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = "SELECT * FROM employees"
        try:
            cursor.execute(sql)
            employees = cursor.fetchall()
            return employees
        finally:
            cursor.close()
            conn.close()

    def update_employee(self, employee_id, data):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = """
            UPDATE employees 
            SET first_name = %s, last_name = %s, email = %s, 
                department_id = %s, is_it_staff = %s
            WHERE employee_id = %s
        """
        try:
            cursor.execute(sql, (
                data['first_name'], data['last_name'], data['email'], 
                data['department_id'], data['is_it_staff'], employee_id
            ))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            cursor.close()
            conn.close()
            
    def delete_employee(self, employee_id):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = "DELETE FROM employees WHERE employee_id = %s"
        try:
            cursor.execute(sql, (employee_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            cursor.close()
            conn.close()

    # ----------------------------------------
    # II. ЗВІТИ (Report Queries)
    # ----------------------------------------

    # 1. Звіт M:1: Працівники по відділах
    def get_employees_by_department(self, department_id):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = """
            SELECT 
                e.employee_id, e.first_name, e.last_name, e.email, d.name AS department_name
            FROM employees e
            JOIN departments d ON e.department_id = d.department_id
            WHERE d.department_id = %s
            ORDER BY e.last_name
        """
        try:
            cursor.execute(sql, (department_id,))
            employees = cursor.fetchall()
            return employees
        finally:
            cursor.close()
            conn.close()
    
    # 2. Звіт M:M: Призначення заявки
    def get_assignments_for_ticket(self, ticket_id):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = """
            SELECT 
                ta.assignment_id, e.first_name, e.last_name, e.email, ta.role, ta.assigned_at
            FROM ticket_assignments ta
            JOIN employees e ON ta.assignee_id = e.employee_id
            WHERE ta.ticket_id = %s
            ORDER BY ta.assigned_at DESC
        """
        try:
            cursor.execute(sql, (ticket_id,))
            assignments = cursor.fetchall()
            return assignments
        finally:
            cursor.close()
            conn.close()
            
    # 3. Звіт з Групуванням: Кількість обладнання за типом
    def get_equipment_count_by_type(self):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = """
            SELECT 
                et.name AS type_name, 
                COUNT(e.equipment_id) AS total_count,
                SUM(CASE WHEN e.status = 'in_use' THEN 1 ELSE 0 END) AS in_use_count,
                GROUP_CONCAT(e.model SEPARATOR ', ') AS models_used
            FROM equipment_types et
            JOIN equipment e ON et.equipment_type_id = e.equipment_type_id
            GROUP BY et.name
            ORDER BY total_count DESC
        """
        try:
            cursor.execute(sql)
            report = cursor.fetchall()
            return report
        finally:
            cursor.close()
            conn.close()
            
    # ----------------------------------------
    # III. ЗАВДАННЯ ЛР №5
    # ----------------------------------------

    # 1. Тригер: Цілісність 1:M (IT_Specialization -> departments)
# app/dao/employee_dao.py (Виправлений блок create_specialization)

    def create_specialization(self, data):
        conn = None
        cursor = None
        sql = """
            INSERT INTO IT_Specialization 
            (department_id, name, required_certifications) 
            VALUES (%s, %s, %s)
        """
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Виконання запиту
            cursor.execute(sql, (
                data.get('department_id'), 
                data['name'], 
                data.get('required_certifications')
            ))
            conn.commit()
            return cursor.lastrowid
            
        except (pymysql.err.InternalError, pymysql.err.IntegrityError) as e:
            # Обробка помилок тригера або цілісності
            if conn:
                conn.rollback()
            return str(e) 
            
        except Exception as e:
            # Загальна помилка Python або з'єднання
            print(f"Non-DB/Unhandled Error creating specialization: {e}")
            if conn:
                conn.rollback()
            return None # Повертаємо None, щоб спрацював generic 500 у контролері
            
        finally:
            # Безпечне закриття з'єднання
            if cursor:
                cursor.close()
            if conn:
                conn.close()

# app/dao/employee_dao.py (ДОДАТИ всередині класу EmployeeDAO)

    # 2.a. SP: Параметризована вставка (equipment_types)
    def create_equipment_type_sp(self, name):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Виклик процедури та передача вхідного параметра
        sql_call = "CALL sp_insert_equipment_type(%s, @new_id)"
        
        try:
            cursor.execute(sql_call, (name,))
            # Отримання вихідного параметра @new_id
            cursor.execute("SELECT @new_id")
            new_id = cursor.fetchone()['@new_id']
            conn.commit()
            return new_id
        except Exception as e:
            print(f"Error calling sp_insert_equipment_type: {e}")
            conn.rollback()
            return None
        finally:
            cursor.close()
            conn.close()

# app/dao/employee_dao.py (ДОДАТИ всередині класу EmployeeDAO)

    # 2.b. SP: M:M Вставка за значеннями (ticket_assignments)
    def assign_ticket_sp(self, assignee_fname, assignee_lname, ticket_title, role):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        sql_call = "CALL sp_assign_ticket_by_names(%s, %s, %s, %s, @assignment_id)"
        
        try:
            # Виконання процедури
            cursor.execute(sql_call, (assignee_fname, assignee_lname, ticket_title, role))
            
            # Отримання вихідного параметра @assignment_id
            cursor.execute("SELECT @assignment_id")
            assignment_id = cursor.fetchone()['@assignment_id']
            conn.commit()
            return assignment_id
            
        except Exception as e:
            print(f"Error calling sp_assign_ticket_by_names: {e}")
            conn.rollback()
            return None
        finally:
            cursor.close()
            conn.close()

# app/dao/employee_dao.py (ДОДАТИ всередині класу EmployeeDAO)

    # 2.c. SP: Пакетна вставка (equipment_types)
    def batch_insert_equipment_types_sp(self, start_id=4):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = "CALL sp_batch_insert_equipment_types(%s, @rows_inserted)"
        try:
            cursor.execute(sql, (start_id,))
            
            # Отримання вихідного параметра
            cursor.execute("SELECT @rows_inserted")
            rows = cursor.fetchone()['@rows_inserted']
            conn.commit()
            return rows
        except Exception as e:
            print(f"Error executing sp_batch_insert_equipment_types: {e}")
            conn.rollback()
            return None
        finally:
            cursor.close()
            conn.close()

# app/dao/employee_dao.py (ДОДАТИ всередині класу EmployeeDAO)

    # 2.d. SP + UDF: Агрегація (tickets.priority_id)
    def get_ticket_priority_stats_sp(self):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = "CALL sp_report_ticket_priority_stats()"
        try:
            cursor.execute(sql)
            stats = cursor.fetchone() # Процедура повертає один рядок звіту
            conn.commit()
            return stats
        except Exception as e:
            print(f"Error executing sp_report_ticket_priority_stats: {e}")
            conn.rollback()
            return None
        finally:
            cursor.close()
            conn.close()

# app/dao/employee_dao.py (ДОДАТИ всередині класу EmployeeDAO)

    # 2.e.i. SP з курсором: Динамічний розподіл даних
    def split_equipment_log_sp(self):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # NOTE: Ця процедура створює нові таблиці в БД
        sql = "CALL sp_split_equipment_log(@rows_moved)"
        try:
            cursor.execute(sql)
            
            # Отримання вихідного параметра
            cursor.execute("SELECT @rows_moved")
            rows = cursor.fetchone()['@rows_moved']
            conn.commit()
            
            # Для перевірки: отримати список нових таблиць (необов'язково)
            cursor.execute("SHOW TABLES LIKE 'equipment_log_%'")
            new_tables = [t[list(t.keys())[0]] for t in cursor.fetchall()]

            return {'rows_moved': rows, 'new_tables': new_tables}
        except Exception as e:
            print(f"Error executing sp_split_equipment_log: {e}")
            conn.rollback()
            return None
        finally:
            cursor.close()
            conn.close()

# app/dao/employee_dao.py (Метод delete_employee)

    def delete_employee(self, employee_id):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = "DELETE FROM employees WHERE employee_id = %s"
        try:
            cursor.execute(sql, (employee_id,))
            conn.commit()
            return cursor.rowcount > 0 # Якщо спрацював тригер, це буде 0 або помилка
        except Exception as e: # <--- ТРИГЕР БУДЕ СХВАЧЕНИЙ ТУТ!
            conn.rollback()
            # ПОВЕРНЕННЯ ТЕКСТУ ПОМИЛКИ ТРИГЕРА:
            return str(e) # Повертаємо рядок, а не True/False
        finally:
            cursor.close()
            conn.close()

# app/dao/employee_dao.py (ДОДАТИ всередині класу EmployeeDAO)

    # 3.b. DELETE для перевірки кардинальності (equipment_types)
    def delete_equipment_type(self, type_id):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = "DELETE FROM equipment_types WHERE equipment_type_id = %s"
        try:
            cursor.execute(sql, (type_id,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            # Повертаємо текст помилки тригера
            return str(e)
        finally:
            cursor.close()
            conn.close()

# app/dao/employee_dao.py (ДОДАТИ всередині класу EmployeeDAO)

    # 3.c. Отримання логів (для перевірки логування)
    def get_equipment_type_deletion_logs(self):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        sql = "SELECT * FROM equipment_type_deletion_log ORDER BY log_id DESC"
        try:
            cursor.execute(sql)
            logs = cursor.fetchall()
            return logs
        except Exception as e:
            print(f"Error fetching logs: {e}")
            return None
        finally:
            cursor.close()
            conn.close()