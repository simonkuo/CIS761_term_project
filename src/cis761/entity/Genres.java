package cis761.entity;

public class Genres {
    int id;
    String name;

    public Genres(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return this.id;
    }
    public String getName() {
        return this.name;
    }
}
