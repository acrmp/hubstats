package hubstats;

public interface EventExtractor {

    boolean extract(String text, Event.Builder builder);

    EventType getEventType();

}
