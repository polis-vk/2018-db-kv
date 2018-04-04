package ru.mail.polis;

import com.mysql.fabric.jdbc.FabricMySQLDriver;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.entities.Author;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.NoSuchElementException;


public class Dao implements KVDao{

    //public static final String URL = "jdbc:mysql://localhost:3306/DB_all/Library";
    public static final String URL = "jdbc:mysql://localhost:3306/Library";
    public static final String USER = "root";
    public static final String PASS = "root";
    public static final String DRIVER = "build/libs/mysql-connector-java-5.1.46.jar";
    private Connection connection;

    public Dao() throws SQLException {
//        DriverManager.getDriver(DRIVER);
        Driver driver = new FabricMySQLDriver();
        DriverManager.registerDriver(driver);
        this.connection = DriverManager.getConnection(URL, USER, PASS);
    }

    @NotNull
    @Override
    public byte[] get(byte[] key) throws NoSuchElementException, IOException {
        String sql = "SELECT * FROM Library.Author WHERE idAuthor = ?;";
        Author author = new Author();
        try (PreparedStatement stm = connection.prepareStatement(sql)) {
            String str = new String(key);
            stm.setInt(1, Integer.parseInt(new String(key)));
            ResultSet rs = stm.executeQuery();
            rs.next();
            author.setId(rs.getInt("idAuthor"));
            author.setFirstName(rs.getString("First Name"));
            author.setLastName(rs.getString("Last Name"));
            author.setCountry(rs.getString("country"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return author.toString().getBytes();
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        String str = new String(value);
        Author author = new Author();
        author.fromString(str);
/*        String sql = "UPDATE Library.Author SET FirstName = ?, LastName = ?, country = ? WHERE idAuthor = ?;";
        try (PreparedStatement stm = connection.prepareStatement(sql);) {
            stm.setString(1, author[0]);
            stm.setString(2, author[1]);
            stm.setString(3, author[2]);
            stm.setInt(4, key[0]);
            int count = stm.executeUpdate();
            if (count != 1) {
                throw new SQLException("On update modify more then 1 record: " + count);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }*/
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        String sql = "DELETE FROM Library.Author WHERE idAuthor = ?;";
        try (PreparedStatement stm = connection.prepareStatement(sql)) {
            stm.setInt(1, key[0]);
            int count = stm.executeUpdate();
            if (count != 1) {
                throw new SQLException("On update modify more then 1 record: " + count);
            }
            stm.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            this.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
