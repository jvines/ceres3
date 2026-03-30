project('ceres3', 'c', 'fortran',
    version: '1.0.5',
    default_options: ['warning_level=1'],
)

py = import('python').find_installation(pure: false)

# NumPy include directory
numpy_inc = run_command(py, ['-c', 'import numpy; print(numpy.get_include())'],
    check: true).stdout().strip()
numpy_dep = declare_dependency(include_directories: numpy_inc)

# GSL dependency
gsl_dep = dependency('gsl', required: true)

# Extension subdirectory
subdir('src/ceres3/ext')

# Install Python source tree
install_subdir('src/ceres3',
    install_dir: py.get_install_dir(),
    strip_directory: false,
)
