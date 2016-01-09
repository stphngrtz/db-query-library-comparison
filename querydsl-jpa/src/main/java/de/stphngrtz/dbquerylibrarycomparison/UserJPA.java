package de.stphngrtz.dbquerylibrarycomparison;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "users")
public class UserJPA extends User {

    private List<RoleJPA> roles = new ArrayList<>();

    public UserJPA() {
        super(null, null, null);
    }

    public UserJPA(Integer id, String name, String email) {
        super(id, name, email);
    }

    @Id
    @Column(name = "id")
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Column(name = "name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "email")
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE})
    @JoinTable(name = "users_with_roles", joinColumns = @JoinColumn(name = "user_id"), inverseJoinColumns = @JoinColumn(name = "role_id"))
    public List<RoleJPA> getRoles() {
        return roles;
    }

    public void setRoles(List<RoleJPA> roles) {
        this.roles = roles;
    }
}
