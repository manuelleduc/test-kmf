package fr.mleduc.kmf.test.kmf;


import etm.core.configuration.BasicEtmConfigurator;
import etm.core.configuration.EtmManager;
import etm.core.monitor.EtmMonitor;
import etm.core.monitor.EtmPoint;
import etm.core.renderer.SimpleTextRenderer;
import gol.GolModel;
import gol.meta.MetaCell;
import org.jdeferred.Deferred;
import org.jdeferred.DeferredManager;
import org.jdeferred.DonePipe;
import org.jdeferred.Promise;
import org.jdeferred.impl.DefaultDeferredManager;
import org.jdeferred.impl.DeferredObject;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;
import org.kevoree.modeling.KObject;
import org.kevoree.modeling.memory.manager.DataManagerBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by mleduc on 17/03/16.
 */
public class Application {
    public static void main(String[] args) throws InterruptedException {

        //Thread.sleep(30000);
        //Thread.sleep(3000);
        // 1 -> init a first list of LifeOperation (life only)
        // 2 -> persist it
        // 3 -> retrieve a cell grid <--------------+
        // 4 -> produce a serie of life actions     |
        // 5 -> persit it                           |
        // 6 -> repeat N time ----------------------+

        BasicEtmConfigurator.configure();
        final EtmMonitor monitor = EtmManager.getEtmMonitor();
        monitor.start();

        final long dim1 = 20;
        final long dim2 = 20;
        final int max = 5;
        final int itts = 1;
        for(int i = 0; i< itts; i++) {
            wholeProcess(monitor, max, i);
        }

        monitor.stop();
        monitor.render(new SimpleTextRenderer());


    }

    private static void wholeProcess(final EtmMonitor monitor, final int max, final int iterationLoop) {
        System.out.println("Start " + iterationLoop);
        EtmPoint point = monitor.createPoint("start");

        final GolModel model = new GolModel(DataManagerBuilder.buildDefault());
        final Deferred<Object, Object, Object> connectDeferred = connect(model);

        connectDeferred.then(o -> {
            final List<LifeOperation> lifeOperations = new ArrayList<>();

            lifeOperations.add(LifeOperation.newCell(0,0));
            lifeOperations.add(LifeOperation.newCell(0,1));
            lifeOperations.add(LifeOperation.newCell(0,2));
            lifeOperations.add(LifeOperation.newCell(1,0));
            lifeOperations.add(LifeOperation.newCell(1,2));
            lifeOperations.add(LifeOperation.newCell(2,0));
            lifeOperations.add(LifeOperation.newCell(2,1));
            lifeOperations.add(LifeOperation.newCell(2,2));

            Promise<Object, Object, Object> firstLifeOperations = proceedLifeOperations(model, 0, lifeOperations)
                    .then((MultipleResults result) -> save(model));
            //final List<int> jksdfjksdf = new Arr
            for (int i = 1; i < max; i++) {
                //long start = System.currentTimeMillis();
                EtmPoint point2 = monitor.createPoint("loop");
                firstLifeOperations = step(model, firstLifeOperations, i);
                point2.collect();

            }

            final Promise<CellGrid, Object, Object> then1 = firstLifeOperations
                    .then((DonePipe<Object, CellGrid, Object, Object>) result -> getAllCells(model, max));

            then1.then((CellGrid aaa ) -> { point.collect();  return aaa; }).done(c -> showState(max, c));
        });
        System.out.println("Stop " + iterationLoop);
    }

    private static Promise<Object, Object, Object> step(final GolModel graph, final Promise<Object, Object, Object> promise, final long lifeI) {
        return promise
                .then((Object result) -> getAllCells(graph, lifeI))
                //.then(c -> {showState(lifeI, c); })
                .then((CellGrid result) -> doLife(result))
                .then((List<LifeOperation> result) -> proceedLifeOperations(graph, lifeI, result))
                .then((MultipleResults result) -> save(graph))
                ;
    }

    private static void showState(long lifeI, CellGrid c) {
        System.out.println("State at " + lifeI);
        System.out.println(c);
    }

    private static Promise<List<LifeOperation>, Object, Object> doLife(final CellGrid result) {
        final DeferredObject<List<LifeOperation>, Object, Object> deferred = new DeferredObject<>();
        final List<LifeOperation> lifeOperations = new GameOfLifeService().doLife(result);
        deferred.resolve(lifeOperations);
        return deferred;
    }

    private static Deferred<KObject[], Object, Object> getAllNodes(final GolModel graph, final long time) {
        final Deferred<KObject[], Object, Object> ret = new DeferredObject<>();
        graph.findAll(MetaCell.getInstance(), 0, time, ret::resolve);
        return ret;
    }

    private static Promise<CellGrid, Object, Object> getAllCells(final GolModel graph, final long time) {
        final DeferredObject<CellGrid, Object, Object> ret = new DeferredObject<>();
        getAllNodes(graph, time).then(result -> {
            final List<Cell> lstCells = Arrays.asList(result).stream()
                    .map(kNode -> {
                        final long x = (long) kNode.get(MetaCell.ATT_X);
                        final long y = (long) kNode.get(MetaCell.ATT_Y);
                        return new Cell(x, y);
                    })
                    .collect(Collectors.toList());
            ret.resolve(new CellGrid(lstCells));
        });
        return ret;
    }

    private static Promise<MultipleResults, OneReject, MasterProgress> proceedLifeOperations(final GolModel model, final long time, final List<LifeOperation> lifeOperations) {
        final Deferred[] res = new Deferred[lifeOperations.size()];
        lifeOperations.stream().map(lifeOperation -> {
            final Promise<Object, Object, Object> ret;
            if (lifeOperation.type == LifeOperation.LifeOperationType.New) {
                // Life
                final gol.Cell cell = createCell(model, time, lifeOperation.x, lifeOperation.y);
                final DeferredObject<Object, Object, Object> tmp = new DeferredObject<>();
                tmp.resolve(null);
                ret = tmp;
            } else {
                // Death
                ret = removeCell(model, time, lifeOperation.x, lifeOperation.y);
            }
            return ret;
        }).collect(Collectors.toList()).toArray(res);

        final DeferredManager barrierIndexes = new DefaultDeferredManager();
        return barrierIndexes.when(res);
    }

    private static Promise<Object, Object, Object> removeCell(final GolModel graph, final long saveTime, final long x, final long y) {
        final Promise<Object, Object, Object> res = lookupCellByCoordinates(graph, saveTime, x, y)
                .then((DonePipe<KObject, Object, Object, Object>) cell -> {
                    final Deferred<Object, Object, Object> ret = new DeferredObject<>();

                    cell.detach(ret::resolve);
                    /*if (cell == null) {
                        System.out.println("Cell(" + x + ", " + y + ") not found");
                    } else {
                        graph.unindex("cells", cell, new String[]{"x", "y"}, ret::resolve);
                    }*/
                    return ret;
                });
        return res;
    }

    private static Deferred<KObject, Object, Object> lookupCellByCoordinates(final GolModel model, final long saveTime, final long x, final long y) {
        final Deferred<KObject, Object, Object> deferred = new DeferredObject<>();
        final String query = "x=" + x + ",y=" + y;
        model.find(MetaCell.getInstance(), 0, saveTime, query, deferred::resolve);
        return deferred;
    }

    private static gol.Cell createCell(final GolModel graph, final long time, final long x, final long y) {
        final gol.Cell cell = graph.universe(0).time(time).createCell();
        cell.setX(x);
        cell.setY(y);
        return cell;
    }

    /*private static Promise<Boolean, Object, Object> indexCell(final KGraph graph, KNode cell) {
        final Deferred<Boolean, Object, Object> ret = new DeferredObject<>();
        graph.index("cells", cell, new String[]{"x", "y"}, ret::resolve);
        return ret;
    }*/

    private static Deferred<Object, Object, Object> save(final GolModel model) {
        final Deferred<Object, Object, Object> ret = new DeferredObject<>();
        model.save(ret::resolve);
        return ret;
    }

    private static Deferred<Object, Object, Object> connect(final GolModel graph) {
        final Deferred<Object, Object, Object> ret = new DeferredObject<>();
        graph.connect(ret::resolve);
        return ret;
    }
}
