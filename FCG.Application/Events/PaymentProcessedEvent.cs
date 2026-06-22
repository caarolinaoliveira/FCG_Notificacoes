namespace FCG.Application.Events
{
    public class PaymentProcessedEvent
    {
        public Guid UserId { get; set; }
        public Guid GameId { get; set; }
        public decimal Price { get; set; }
        public string Status { get; set; } = string.Empty;
        public DateTime ProcessedAt { get; set; }
    }
}