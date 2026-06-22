namespace FCG.Application.Events
{
    public class UserRegisteredEvent
    {
        public Guid UserId { get; set; }
        public string Nome { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public DateTime CriadoEm { get; set; }
    }
}