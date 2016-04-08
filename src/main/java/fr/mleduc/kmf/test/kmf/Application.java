package fr.mleduc.kmf.test.kmf;


import etm.core.configuration.BasicEtmConfigurator;
import etm.core.configuration.EtmManager;
import etm.core.monitor.EtmMonitor;
import etm.core.monitor.EtmPoint;
import etm.core.renderer.SimpleTextRenderer;
import gol.GolModel;
import gol.meta.MetaCell;
import org.kevoree.modeling.KObject;
import org.kevoree.modeling.memory.manager.DataManagerBuilder;
import org.kevoree.modeling.scheduler.impl.DirectScheduler;
import rx.Observable;

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

        final int max = 10000;
        final int itts = 1;

        generateRange(itts).map(i -> {
            wholeProcess(monitor, max, i);
            return null;
        }).toBlocking().last();

        monitor.stop();
        monitor.render(new SimpleTextRenderer());


    }

    private static Observable<Integer> generateRange(int itts) {
        return Observable.create(subscriber -> {
            for (int i = 0; i < itts; i++) {
                subscriber.onNext(i);
            }
            subscriber.onCompleted();
        });
    }

    private static <T> Observable<T> streamer(final List<T> lst) {
        return Observable.create(subscriber -> {
            for (T t : lst) {
                subscriber.onNext(t);
            }
            subscriber.onCompleted();
        });
    }

    private static void wholeProcess(final EtmMonitor monitor, final int max, final int iterationLoop) {
        //System.out.println("Start whole process " + iterationLoop);
        final EtmPoint point = monitor.createPoint("start");
        final GolModel model = new GolModel(DataManagerBuilder.create().withScheduler(new DirectScheduler()).build());
        connect(model).map(n -> {
            final List<LifeOperation> lifeOperations = new ArrayList<>();

            lifeOperations.add(LifeOperation.newCell(0, 0));
            lifeOperations.add(LifeOperation.newCell(0, 1));
            lifeOperations.add(LifeOperation.newCell(0, 2));
            lifeOperations.add(LifeOperation.newCell(1, 0));
            lifeOperations.add(LifeOperation.newCell(1, 2));
            lifeOperations.add(LifeOperation.newCell(2, 0));
            lifeOperations.add(LifeOperation.newCell(2, 1));
            lifeOperations.add(LifeOperation.newCell(2, 2));

            proceedLifeOperations(model, 0, lifeOperations);
            save(model).toBlocking().last();
            return null;

        }).switchMap(a1 -> generateRange(max)).forEach(x -> {
            step(model, x, monitor);
        });

        getAllCells(model, max)
                .map(tmp -> {
                    showState(max, tmp);
                    return tmp;
                }).toBlocking().last();

        disconnect(model)
                .map(o -> {
                    point.collect();
                    return null;
                })
                .toBlocking().last();
    }

    private static void step(GolModel model, Integer lifeI, EtmMonitor monitor) {
        EtmPoint point2 = monitor.createPoint("loop");
        getAllCells(model, lifeI)
                /*.map(tmp -> {
                    showState(lifeI, tmp);
                    return tmp;
                })*/
                .switchMap(cellGrid -> doLife(cellGrid))
                .map(res -> {
                    proceedLifeOperations(model, lifeI, res);
                    return null;
                })
                .switchMap(o -> save(model))
                .toBlocking().last();
        point2.collect();

    }

    private static void proceedLifeOperations(GolModel model, int time, List<LifeOperation> lifeOperations) {
        streamer(lifeOperations).switchMap(lifeOperation -> {
            final Observable<?> ret;
            if (lifeOperation.type == LifeOperation.LifeOperationType.New) {
                createCell(model, time, lifeOperation.x, lifeOperation.y);
                ret = Observable.create(subscriber -> {
                    subscriber.onNext(null);
                    subscriber.onCompleted();
                });
            } else {
                ret = removeCell(model, time, lifeOperation.x, lifeOperation.y);
            }
            return ret;
        }).toBlocking().last();
    }

    private static void showState(long lifeI, CellGrid c) {
        System.out.println("State at " + lifeI);
        System.out.println(c);
    }

    private static Observable<List<LifeOperation>> doLife(final CellGrid result) {
        return Observable.create(subscriber -> {
            final List<LifeOperation> lifeOperations = new GameOfLifeService().doLife(result);
            subscriber.onNext(lifeOperations);
            subscriber.onCompleted();
        });
    }

    private static Observable<KObject[]> getAllNodes(final GolModel graph, final long time) {
        return Observable.create(subscriber -> {
            graph.findAll(MetaCell.getInstance(), 0, time, kObjects -> {
                subscriber.onNext(kObjects);
                subscriber.onCompleted();
            });

        });
    }

    private static Observable<CellGrid> getAllCells(final GolModel graph, final long time) {
        return getAllNodes(graph, time).map(result -> {
            final List<Cell> lstCells = Arrays.asList(result).stream()
                    .map(kNode -> {
                        final long x = (long) kNode.get(MetaCell.ATT_X);
                        final long y = (long) kNode.get(MetaCell.ATT_Y);
                        return new Cell(x, y);
                    })
                    .collect(Collectors.toList());
            return new CellGrid(lstCells);
        });
    }


    private static Observable<Object> removeCell(final GolModel graph, final long saveTime, final long x, final long y) {
        return lookupCellByCoordinates(graph, saveTime, x, y).switchMap(Application::detachCell);
    }

    private static Observable<Object> detachCell(KObject kObject) {
        return Observable.create(subscriber -> {
            kObject.detach(o -> {
                subscriber.onNext(o);
                subscriber.onCompleted();
            });
        });
    }

    private static Observable<KObject> lookupCellByCoordinates(final GolModel model, final long saveTime, final long x, final long y) {
        final String query = "x=" + x + ",y=" + y;
        return Observable.create(subscriber -> {
            model.find(MetaCell.getInstance(), 0, saveTime, query, kObject -> {
                subscriber.onNext(kObject);
                subscriber.onCompleted();
            });
        });
    }

    private static gol.Cell createCell(final GolModel graph, final long time, final long x, final long y) {
        final gol.Cell cell = graph.universe(0).time(time).createCell();
        cell.setX(x);
        cell.setY(y);
        return cell;
    }

    private static Observable<Object> save(final GolModel model) {
        return Observable.create(subscriber -> {
            model.save(o -> {
                subscriber.onNext(o);
                subscriber.onCompleted();
            });
        });
    }

    private static Observable<Object> connect(final GolModel graph) {
        return Observable.create(subscriber -> {
            graph.connect(o -> {
                subscriber.onNext(o);
                subscriber.onCompleted();
            });
        });
    }

    private static Observable<Object> disconnect(final GolModel graph) {
        return Observable.create(subscriber -> {
            graph.disconnect(o -> {
                subscriber.onNext(o);
                subscriber.onCompleted();
            });
        });
    }
}
