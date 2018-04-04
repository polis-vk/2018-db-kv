package ru.mail.polis.entities;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Author implements Serializable {
    private int id;
    private String firstName;
    private String lastName;
    private String country;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return "Author{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lasName='" + lastName + '\'' +
                ", country='" + country + '\'' +
                '}';
    }

    public void fromString(String str) {

        String digitP = "=(.*),";
        String stringP = "'(.*)'";

/*        String pattern = "=(.)*,";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(str);
            id = Integer.parseInt(m.group(1));
            firstName = m.group(1);
            lastName = m.group(2);
            country = m.group(3);
        }*/
        Pattern r = Pattern.compile(digitP);
        Pattern s = Pattern.compile(stringP);

        Matcher m = s.matcher(str);
        id = Integer.parseInt(s.matcher(str).group(1));
        firstName = m.group(2);
        lastName = m.group(3);
        country = m.group(4);

    }
}