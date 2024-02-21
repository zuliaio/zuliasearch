package io.zulia.util.iterator;

import org.slf4j.Logger;

import java.util.Iterator;
import java.util.function.Consumer;

public class StatusIterator<T> implements Iterable<T> {

	private final Iterator<T> iterator;
	private final Consumer<String> log;
	private final String messagePrefix;
	private final int everyN;

	private int counter;

	public static <T> StatusIterator<T> infoLogIterator(Iterable<T> iterable, Logger logger, String messagePrefix, int everyN) {
		return new StatusIterator<>(iterable, logger::info, messagePrefix, everyN);
	}

	public static <T> StatusIterator<T> soutIterator(Iterable<T> iterable, String messagePrefix, int everyN) {
		return new StatusIterator<>(iterable, System.out::println, messagePrefix, everyN);
	}

	public StatusIterator(Iterable<T> iterable, Consumer<String> log, String messagePrefix, int everyN) {
		this(iterable.iterator(), log, messagePrefix, everyN);
	}

	public StatusIterator(Iterator<T> iterator, Consumer<String> log, String messagePrefix, int everyN) {
		this.iterator = iterator;
		this.log = log;
		this.messagePrefix = messagePrefix;
		this.everyN = everyN;
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public T next() {
				counter++;
				if (counter % everyN == 0) {
					log.accept(messagePrefix + counter);
				}
				return iterator.next();
			}
		};
	}
}
