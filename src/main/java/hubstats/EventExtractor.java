package hubstats;

public interface EventExtractor {

    public boolean extract(String text, Event.Builder builder);

    public EventType getEventType();

}
