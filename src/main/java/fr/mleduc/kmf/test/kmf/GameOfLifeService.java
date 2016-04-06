package fr.mleduc.kmf.test.kmf;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by mleduc on 11/03/16.
 */
public class GameOfLifeService {
    public List<LifeOperation> doLife(final CellGrid cellGrid) {
        return cellGrid.nextTime.stream().flatMap(cell -> {
            final ArrayList<LifeOperation> lifeOperations = new ArrayList<>();
            long x = cell.getX();
            long y = cell.getY();
            if (cellGrid.isAlive(x, y)) {
                long cpt = cellGrid.countNeighbourAlive(x, y);
                if (cpt < 2 || cpt > 3) {
                    lifeOperations.add(LifeOperation.deadCell(cell.getX(), cell.getY()));
                }
            } else {
                long cpt = cellGrid.countNeighbourAlive(x, y);
                if (cpt == 3) {
                    lifeOperations.add(LifeOperation.newCell(x,y));
                }
            }
            return lifeOperations.stream();
        }).collect(Collectors.toList());
    }
}
