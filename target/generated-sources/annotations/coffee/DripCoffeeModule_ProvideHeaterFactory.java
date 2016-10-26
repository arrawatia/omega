package coffee;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import javax.annotation.Generated;

@Generated(
  value = "dagger.internal.codegen.ComponentProcessor",
  comments = "https://google.github.io/dagger"
)
public final class DripCoffeeModule_ProvideHeaterFactory implements Factory<Heater> {
  private final DripCoffeeModule module;

  public DripCoffeeModule_ProvideHeaterFactory(DripCoffeeModule module) {
    assert module != null;
    this.module = module;
  }

  @Override
  public Heater get() {
    return Preconditions.checkNotNull(
        module.provideHeater(), "Cannot return null from a non-@Nullable @Provides method");
  }

  public static Factory<Heater> create(DripCoffeeModule module) {
    return new DripCoffeeModule_ProvideHeaterFactory(module);
  }
}
