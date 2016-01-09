package de.stphngrtz.dbquerylibrarycomparison;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "roles")
public class RoleJPA extends Role {

    private List<UserJPA> user = new ArrayList<>();

    public RoleJPA() {
        super(null, null);
    }

    public RoleJPA(Integer id, String name) {
        super(id, name);
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

    @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE}, mappedBy = "roles")
    public List<UserJPA> getUser() {
        return user;
    }

    public void setUser(List<UserJPA> user) {
        this.user = user;
    }
}
