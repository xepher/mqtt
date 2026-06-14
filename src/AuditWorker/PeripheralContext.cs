using Microsoft.EntityFrameworkCore;

namespace AuditWorker
{
    public class PeripheralStatus
    {
        public int Id { get; set; }
        public string? DeviceId { get; set; }
        public int BatteryLevel { get; set; }
        public bool SensorsOk { get; set; }
        public DateTime Timestamp { get; set; }
        public string? WorkerName { get; set; }
    }

    public class PeripheralContext : DbContext
    {
        public DbSet<PeripheralStatus> PeripheralStatuses { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlite("Data Source=data/peripheral.db;Cache=Shared;Mode=ReadWriteCreate;Default Timeout=5");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PeripheralStatus>().HasKey(x => x.Id);
            modelBuilder.Entity<PeripheralStatus>().Property(x => x.DeviceId).IsRequired(false);
            modelBuilder.Entity<PeripheralStatus>().Property(x => x.WorkerName).IsRequired(false);
        }
    }
}
