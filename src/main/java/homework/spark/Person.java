package homework.spark;

class Person {
    public String firstName;
    public String lastName;
    public String location;
    public Integer count;

    public Person(String csvData, Integer count) {
        String[] fields = csvData.split(";");
        firstName = fields[0];
        lastName = fields[1];
        location = fields[2];
        this.count = count;
    }
}
