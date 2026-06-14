using Microsoft.EntityFrameworkCore;

namespace CentralController
{
    public class DeviceStatus
    {
        public string DeviceId { get; set; } = string.Empty;
        public bool IsOnline { get; set; }
        public DateTime LastSeen { get; set; }
    }

    public class CommandFeedback
    {
        public int Id { get; set; }
        public string DeviceId { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
    }

    public class ControllerContext : DbContext
    {
        public DbSet<DeviceStatus> DeviceStatuses { get; set; }
        public DbSet<CommandFeedback> CommandFeedbacks { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlite("Data Source=data/controller.db;Cache=Shared;Mode=ReadWriteCreate;Default Timeout=5");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<DeviceStatus>().HasKey(x => x.DeviceId);
            modelBuilder.Entity<CommandFeedback>().HasKey(x => x.Id);
        }
    }
}
