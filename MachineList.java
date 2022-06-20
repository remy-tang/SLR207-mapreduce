import java.io.Serializable;
import java.util.ArrayList;

public class MachineList implements Serializable {

    // Array is made volatile to communicate changes across threads. 
    private volatile ArrayList<String> machines = new ArrayList<>();

    public MachineList() {}
    public MachineList(ArrayList<String> machines) {
        this.machines.addAll(machines);
    }

    public void setMachines(ArrayList<String> machines) {
        this.machines.clear();
        this.machines.addAll(machines);
    }

    public ArrayList<String> getMachines() {
        return this.machines;
    }

    public String getMachine(int index) {
        if (index>=0 && index<=machines.size()) {
            return machines.get(index);
        } else {
            return "Index out of range";
        }
    }
}
